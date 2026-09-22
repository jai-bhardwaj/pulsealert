# PulseAlert

**Set a price alert in 10 seconds. Get pinged the moment it moves.**

PulseAlert is a consumer app anyone can use with zero sign-up: pick an asset (BTC / ETH / SOL), say
"tell me if it drops 1% within 5 minutes", and get a browser notification the moment it happens —
computed continuously by **Apache Flink on Confluent Cloud**, not by polling.

```
Coinbase API ──HTTP Source V2 connector──▶ crypto.spot.raw ──Flink──▶ price_ticks ──Flink (5-min HOP window)──▶ price_moves ─┐
                                                                                                                              ├─Flink interval join──▶ alerts ──SSE──▶ Browser notification
Next.js app ──(user creates rule)──────────────────────────────────▶ alert_rules ────────────────────────────────────────────┘

Every topic has a JSON Schema in Schema Registry (Stream Governance). Lineage is visible end-to-end in the Confluent UI.
```

## Confluent features used

| Feature | Where |
|---|---|
| **Fully-managed Connector** | HTTP Source V2 polls Coinbase spot prices every 10s → `crypto.spot.raw` (no code) |
| **Stream Processing / Flink SQL** | `flink/pipeline.sql`: typed ticks → 5-minute sliding-window % change (HOP) → interval join with per-user rules |
| **Stream Governance** | JSON Schema on all 5 topics via Schema Registry; Stream Lineage graph shows connector → Flink → app; topics tagged in Stream Catalog |
| **Kafka as the app's write path** | The app publishes user rules *into Kafka*; Flink picks them up with no redeploy — rules are data, not code |

---

## Setup (≈ 60–75 minutes end to end)

### 0. Confluent Cloud (15 min)

1. **Environment** → Add environment → name `pulsealert`, Stream Governance package **Essentials** (free).
2. **Cluster** → Create cluster → **Basic** → AWS **us-east-1** (Flink is available there) → name `pulsealert`.
3. **Cluster API key** → Cluster ▸ API keys ▸ Create key (Global access). Save key/secret → `KAFKA_API_KEY/SECRET`.
   Bootstrap server: Cluster ▸ Cluster settings ▸ *Bootstrap server* → `KAFKA_BOOTSTRAP`.
4. **Schema Registry credentials** → Environment page ▸ right-hand *Stream Governance API* panel ▸ copy **Endpoint** → `SR_URL`;
   *Credentials ▸ Add key* → `SR_API_KEY/SECRET`.
5. **Flink compute pool** → Environment ▸ Flink ▸ Create compute pool → same region (us-east-1), max 10 CFU.

### 1. App + topics (5 min)

```bash
npm install
cp .env.example .env      # fill in the 6 values from step 0
npm run setup:topics      # creates crypto.spot.raw and alert_rules
```

### 2. Connector (10 min)

Cluster ▸ Connectors ▸ search **HTTP Source V2** ▸ use the values in
[`connectors/http-source-v2.json`](connectors/http-source-v2.json):

- Details: the wizard needs an OpenAPI spec first — *Add a file* ▸ upload
  [`connectors/coinbase-openapi.yaml`](connectors/coinbase-openapi.yaml)
- Kafka access: *My account* ▸ *Generate API key and download*
- HTTP API Base URL: `https://api.coinbase.com`, Auth type: none (both prefilled from the spec)
- Paths: tick all **3** — `/v2/prices/BTC-USD/spot`, `/v2/prices/ETH-USD/spot`, `/v2/prices/SOL-USD/spot`;
  for each, turn off *Create a new topic* and pick `crypto.spot.raw`; under *Settings* set initial offset `0` and
  request interval `10000` ms (the default is 60000)
- Advanced configuration ▸ output record format: **JSON_SR**
- Tasks: **3** (fixed — one per path; ≈ $1.08/hr, so pause the connector when you're not demoing)

Wait for status **Running**, then open topic `crypto.spot.raw` ▸ Messages — you should see
`{"data":{"amount":"...","base":"BTC","currency":"USD"}}` every ~10 s.

> **Plan B (if the connector misbehaves — do not burn time debugging it):** `npm run producer` publishes the exact same
> records to the same topic + schema from your laptop. Keep it running in a terminal. Mention in the submission that the
> managed connector is the production path.

### 3. Flink SQL (15 min)

Environment ▸ Flink ▸ **Open SQL workspace**. Top-left: catalog = `pulsealert` env, database = `pulsealert` cluster.
Run the statements in [`flink/pipeline.sql`](flink/pipeline.sql) **one at a time**, in order:

1. The sanity `SELECT` on `` `crypto.spot.raw` `` (stop it once rows appear).
2. `CREATE TABLE price_ticks …` then `INSERT INTO price_ticks …` (leave running).
3. `CREATE TABLE price_moves … AS SELECT …` (leave running — first rows appear after ~1–2 min).
4. `CREATE TABLE alerts … AS SELECT …` (leave running).

Environment ▸ Flink ▸ *Flink statements* should now show 3 **Running** statements. Screenshot this.

### 4. Run the app (5 min)

```bash
npm run dev          # http://localhost:3000
```

- Prices populate within ~10 s; 5-minute % change appears after ~1–2 min.
- Click **Enable browser notifications**.
- Create a rule, e.g. *BTC drops 0.05 %* (use a tiny threshold so it fires during the demo).
- Open topic `alert_rules` in Confluent — your rule is there, with its schema.
- When a matching window closes, the alert shows in the feed **and** as a macOS/Windows notification.

**Forcing an alert for the video:** `npm run spike` publishes 90 s of BTC ticks 3 % below market
(clearly labelled *simulated flash crash*). Within ~1–3 min Flink emits the alert (Confluent Flink commits results at
checkpoints, so each hop adds up to a minute) — cut the wait in the edit. Say on camera that it's a simulation.

### 5. Governance touches (5 min — these are cheap points)

- Environment ▸ **Stream Lineage** on `alerts` → screenshot the full graph (connector → 3 Flink statements → app).
- Environment ▸ **Schemas** → screenshot the 5 subjects.
- Topic ▸ *Tags* → add `public-market-data` on `crypto.spot.raw`, `user-generated` on `alert_rules` (Stream Catalog).
- Optional: enable Data Quality Rules on `alert_rules` (e.g. `threshold_pct > 0`).

---

## Project layout

```
app/page.tsx               dashboard (client) — live cards, rule form, alert feed, notifications
app/api/rules/route.ts     POST → registers JSON Schema, produces rule to alert_rules
app/api/stream/route.ts    SSE → tails price_ticks / price_moves / alerts to the browser
lib/kafka.ts               kafkajs client + Schema Registry wire-format helpers
scripts/setup-topics.ts    creates the two non-Flink topics
scripts/producer.ts        fallback producer + simulated-spike mode
flink/pipeline.sql         the whole stream processing pipeline
connectors/http-source-v2.json
docs/SUBMISSION.md         copy-paste answers for the form
docs/DEMO_SCRIPT.md        90-second video shot list
```

## Why streaming (and not a cron job)

A poll-and-compare cron job re-reads history on every tick, can't scale to many users × many rules, and is late by its
interval. Here the connector ingests once, Flink maintains the sliding windows incrementally and joins against rules as
they arrive, and adding a user costs one small Kafka record. Price move to notification takes about 2–3 minutes end to
end (Flink commits results exactly-once at checkpoints), and that stays the same whether the pipeline serves 1 or
1,000,000 users.

## Roadmap

- Stocks / FX / any HTTP API — it's a connector config change, not code
- Push notifications (Web Push / email) via an HTTP Sink connector on `alerts`
- Per-user rule management as a compacted topic (delete = tombstone)
- Pattern alerts with `MATCH_RECOGNIZE` (three consecutive red windows, breakouts)
- Anomaly detection with Flink AI model inference

## License

MIT

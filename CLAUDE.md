# PulseAlert — Claude Code project guide

Real-time price-alert app for the Confluent Dev Day "Laptop Challenge" (deadline: 11:59 PM PDT Sept 22, 2026 =
12:29 PM IST Sept 23). Goal: a working demo + 90s video + submission form. Speed over polish; never gold-plate.

## What this is
Coinbase → Confluent HTTP Source V2 connector → `crypto.spot.raw` → Flink SQL (`price_ticks` → `price_moves` 5-min HOP
window → `alerts` interval-joined with `alert_rules`) → Next.js dashboard via SSE → browser notification.
User rules are written *into Kafka* by the app (`POST /api/rules`). Read `README.md` for the full setup run sheet.

## Stack
- Next.js 14 (App Router), TypeScript, React 18, plain CSS in `app/globals.css`
- `kafkajs` for Kafka; Schema Registry via plain REST (`lib/kafka.ts` handles the 5-byte JSON_SR wire format)
- No ORM, no DB, no auth — state lives in Kafka topics and browser localStorage
- Flink SQL runs in Confluent Cloud, not locally (`flink/pipeline.sql`)

## Commands
```bash
npm install
cp .env.example .env        # KAFKA_BOOTSTRAP, KAFKA_API_KEY/SECRET, SR_URL, SR_API_KEY/SECRET
npm run setup:topics        # creates crypto.spot.raw + alert_rules
npm run dev                 # http://localhost:3000
npm run build               # type-check + build; run before recording the demo
npm run producer            # fallback: poll Coinbase from laptop if the managed connector isn't working
npm run spike               # 90s simulated -3% BTC move to force an alert during the demo
```

## Key files
- `app/page.tsx` — the whole UI (client component)
- `app/api/rules/route.ts` — publishes a rule to `alert_rules` with a registered JSON Schema
- `app/api/stream/route.ts` — SSE endpoint tailing `price_ticks`, `price_moves`, `alerts`
- `lib/kafka.ts` — client, topic names, SR helpers, schemas
- `flink/pipeline.sql` — 3 continuous Flink statements; run one at a time in the Confluent SQL workspace
- `connectors/http-source-v2.json` — connector values
- `docs/SUBMISSION.md`, `docs/DEMO_SCRIPT.md` — form answers and video shot list

## Conventions & gotchas
- Topic names and symbols are constants in `lib/kafka.ts`; change them there only.
- Flink output tables MUST use `'value.format' = 'json-registry'` (default is Avro, which the app can't decode).
- `data` is a reserved word in Flink SQL — always backtick it when reading `crypto.spot.raw`.
- Streaming Flink can't `ORDER BY` a non-time column; don't add it to debug queries.
- Kafka timestamps from Flink arrive as `"YYYY-MM-DD HH:mm:ss.SSSZ"` strings; `fmtTime` in `page.tsx` handles that.
- `alert_rules` is append-only; deleting a rule is client-side only (documented as roadmap).
- `.env` is gitignored. Never commit credentials; never print secrets in logs.
- Don't add dependencies unless something is broken. No UI libraries.

## When helping
1. If something fails, first check `.env` values and that the Flink statements show **Running** in Confluent.
2. Prefer the smallest fix that unblocks the demo; note nicer solutions in README "Roadmap" instead of building them.
3. Keep README/SUBMISSION accurate if behaviour changes — the judges read them.

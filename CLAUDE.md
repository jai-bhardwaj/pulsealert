# PulseAlert — Claude Code project guide

Real-time price-alert app for the Confluent Dev Day "Laptop Challenge" (deadline: 11:59 PM PDT Sept 22, 2026 =
12:29 PM IST Sept 23). Goal: a working demo + 90s video + submission form. Speed over polish; never gold-plate.

## What this is
Coinbase → Confluent HTTP Source V2 connector → `crypto.spot.raw` → Flink SQL (`price_ticks` → `price_moves` 5-min HOP
window → `alerts` interval-joined with `alert_rules`) → Next.js dashboard via SSE → browser notification.
User rules are written *into Kafka* by the app (`POST /api/rules`). Read `README.md` for the full setup run sheet.

## Stack
- Next.js 16 (App Router, Turbopack), TypeScript 7, React 19, plain CSS in `app/globals.css`
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
- Flink `json-registry` timestamps arrive as epoch-millis numbers; `fmtTime` in `page.tsx` handles numbers and strings.
- HOP `window_start`/`window_end` are zone-less (shifted by the workspace time zone, e.g. +5:30 IST) — use `$rowtime` for real instants.
- Tables with `DISTRIBUTED BY` need `'value.fields-include' = 'all'`, or the key columns (e.g. `symbol`) are missing from the value the app reads.
- `FIRST_VALUE`/`LAST_VALUE` aren't supported in HOP windows; `price_moves` uses MIN/MAX over `'ts|price'` strings instead.
- `alert_rules` is append-only; deleting a rule is client-side only (documented as roadmap).
- `.env` is gitignored. Never commit credentials; never print secrets in logs.
- Don't add dependencies unless something is broken. No UI libraries.

## When helping
1. If something fails, first check `.env` values and that the Flink statements show **Running** in Confluent.
2. Prefer the smallest fix that unblocks the demo; note nicer solutions in README "Roadmap" instead of building them.
3. Keep README/SUBMISSION accurate if behaviour changes — the judges read them.

<!-- BEGIN:nextjs-agent-rules -->

# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->

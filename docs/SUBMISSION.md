# Submission — copy/paste answers for cnfl.io/devdaywinner

> Adapt to the actual form fields. Keep the impact paragraph first; judges skim.

## Project name
PulseAlert — real-time price alerts anyone can set in 10 seconds

## One-line description
A zero-sign-up web app where anyone sets "notify me if BTC drops 1% in 5 minutes" and gets a browser notification the
moment it happens — powered end to end by Confluent Cloud: a managed HTTP Source connector, Flink SQL sliding windows and
joins, and Schema Registry governance.

## Problem & business impact (customer experience / time to market)
Retail investors and small treasury teams miss fast market moves because consumer apps either poll slowly, cap alerts,
or only support fixed price levels. Building real-time alerting in-house normally means writing a bespoke consumer,
managing windowed state, and redeploying code every time a user changes a rule.

PulseAlert shows how a data streaming platform turns that into a product feature in an afternoon:

- **Customer experience:** move-to-notification latency is seconds, alerts are relative (% over a sliding window) rather
  than static price lines, and there is no sign-up friction.
- **Time to market:** ingestion is a connector config, the entire analytic is ~40 lines of Flink SQL, and user rules
  are *data in a Kafka topic*, so adding a rule — or a million users — needs no redeploy.
- **Extensible by configuration:** swapping Coinbase for a stock, FX or e-commerce price feed is a change to the
  connector, not the app. The same pipeline is a price-drop alert for shoppers or a stock-out alert for merchants.

## How it uses Confluent (feature checklist)
- **Connectors:** fully-managed **HTTP Source V2** polls Coinbase spot prices for BTC/ETH/SOL every 10s → `crypto.spot.raw`.
- **Stream processing (Flink SQL, 3 continuous statements):**
  1. `price_ticks` — typed, cleaned ticks (`INSERT INTO … SELECT` from the connector topic)
  2. `price_moves` — 5-minute **HOP window** sliding every 30s: open/close/low/high/% change per symbol
  3. `alerts` — **interval join** of window results with the `alert_rules` topic (rules only match moves after they were
     created), filtered by each user's direction and threshold
- **Stream Governance:** JSON Schema on all 5 topics via Schema Registry (connector-inferred, Flink-managed, and
  app-registered); Stream Lineage graph shows connector → Flink → app; Stream Catalog tags on source vs user-generated
  topics.
- **Kafka as the application write path:** the Next.js app produces user rules to `alert_rules` with a registered schema;
  the UI consumes `price_moves` and `alerts` via server-sent events and raises browser notifications.

## Architecture
Coinbase → HTTP Source V2 → `crypto.spot.raw` → Flink → `price_ticks` → Flink (HOP 5m/30s) → `price_moves`
→ Flink interval join with `alert_rules` (written by the app) → `alerts` → Next.js SSE → browser notification

## What's next
Web Push/email through an HTTP Sink connector on `alerts`; compacted rules topic for edit/delete; `MATCH_RECOGNIZE`
pattern alerts; Flink AI anomaly scoring; more feeds via additional connectors.

## Links
- GitHub: <repo URL>
- Demo video (90s): <YouTube/Loom URL>
- Screenshots: connector Running, 3 Flink statements Running, Stream Lineage graph, Schemas page, app with fired alert

# 90-second demo video — shot list

Record with QuickTime/Loom, browser + Confluent UI side by side. Talk over it; no music.
Start `npm run dev` and (if you'll simulate) have `npm run spike` ready in a terminal **before** recording.

| t | Screen | Say |
|---|---|---|
| 0:00 | App, prices ticking | "PulseAlert: anyone can set a price alert in ten seconds and get pinged the moment it moves. Every number here is streaming through Confluent Cloud." |
| 0:10 | Confluent ▸ Connectors, HTTP Source V2 **Running**; click into topic messages | "Ingestion is a fully-managed HTTP Source connector polling Coinbase — no code. Records land in `crypto.spot.raw` with a schema in Schema Registry." |
| 0:25 | Flink ▸ statements (3 Running); open `price_moves` statement | "Three Flink SQL statements: clean ticks, a five-minute sliding window every 30 seconds for % change, and an interval join against user rules." |
| 0:40 | Back to app; create rule *BTC drops 0.05 %* | "Creating a rule writes a record to the `alert_rules` topic —" *(switch to topic view, show the message)* "— Flink picks it up live. No redeploy." |
| 0:52 | Terminal `npm run spike` (say it's simulated) | "I'll simulate a 3 % flash crash so we don't wait for the market." |
| 1:05 | App: alert appears + OS notification pops *(cut the ~2–3 min wait in the edit)* | "As soon as a sliding window catches the drop, Flink emits the alert and the user gets a notification — no polling anywhere." |
| 1:15 | Stream Lineage graph on `alerts` | "Full lineage, connector to app, governed by Schema Registry." |
| 1:22 | App | "Swap the connector and this is a price-drop alert for shoppers or a stock-out alert for merchants. Built in an afternoon on Confluent." |

## Screenshots to attach
1. Connector status **Running**
2. Flink statements list — 3 Running
3. Stream Lineage graph
4. Schema Registry subjects (5)
5. App with a fired alert + the notification

## Before you hit record
- Threshold tiny (0.05 %) so a real move may fire without the simulation.
- Notifications enabled in the browser, and macOS Focus/Do Not Disturb **off**.
- Close other tabs; zoom the browser to 110 % for legibility.

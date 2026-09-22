import "dotenv/config";
import { Partitioners } from "kafkajs";
import {
  encodeJsonSR,
  kafka,
  latestSchemaId,
  registerJsonSchema,
  RAW_TICK_SCHEMA,
  SYMBOLS,
  TOPICS,
} from "../lib/kafka";

/**
 * Fallback / demo producer.
 *
 * Primary path is the fully-managed Confluent HTTP Source connector. Use this
 * script only if the connector is not running, or to inject a *simulated*
 * move for the demo video:
 *
 *   npm run producer                 # poll Coinbase every 5s, same record shape as the connector
 *   SPIKE=BTC:-3 npm run producer    # for 90s, publish BTC ticks 3% below market (simulated flash crash)
 *
 * Records use the same JSON Schema subject (`crypto.spot.raw-value`) as the
 * connector so Flink sees one consistent stream.
 */

const POLL_MS = 5_000;
const SPIKE_DURATION_MS = 90_000;

async function fetchSpot(symbol: string) {
  const res = await fetch(`https://api.coinbase.com/v2/prices/${symbol}-USD/spot`, {
    headers: { "User-Agent": "pulsealert-demo" },
  });
  if (!res.ok) throw new Error(`Coinbase ${symbol}: HTTP ${res.status}`);
  return (await res.json()) as { data: { amount: string; base: string; currency: string } };
}

async function main() {
  const spike = process.env.SPIKE ? parseSpike(process.env.SPIKE) : null;
  if (spike) console.log(`SIMULATION: ${spike.symbol} will be published ${spike.pct}% off market for ${SPIKE_DURATION_MS / 1000}s`);

  const subject = `${TOPICS.raw}-value`;
  const schemaId = (await latestSchemaId(subject)) ?? (await registerJsonSchema(subject, RAW_TICK_SCHEMA));
  console.log(`Using schema id ${schemaId} for ${subject}`);

  const producer = kafka().producer({ createPartitioner: Partitioners.DefaultPartitioner });
  await producer.connect();
  const startedAt = Date.now();

  const tick = async () => {
    const results = await Promise.allSettled(SYMBOLS.map((s) => fetchSpot(s)));
    const messages = [];
    for (const r of results) {
      if (r.status !== "fulfilled") { console.warn(r.reason.message); continue; }
      const rec = r.value;
      if (spike && rec.data.base === spike.symbol && Date.now() - startedAt < SPIKE_DURATION_MS) {
        const adjusted = Number(rec.data.amount) * (1 + spike.pct / 100);
        rec.data.amount = adjusted.toFixed(2);
      }
      messages.push({ key: rec.data.base, value: encodeJsonSR(schemaId, rec) });
    }
    if (messages.length) {
      await producer.send({ topic: TOPICS.raw, messages });
      console.log(new Date().toISOString(), messages.map((m) => `${m.key}`).join(" "), "→", TOPICS.raw);
    }
    if (spike && Date.now() - startedAt >= SPIKE_DURATION_MS + POLL_MS) {
      console.log("Simulation finished, exiting.");
      await producer.disconnect();
      process.exit(0);
    }
  };

  await tick();
  setInterval(() => tick().catch((e) => console.error(e)), POLL_MS);
}

function parseSpike(s: string) {
  const [symbol, pct] = s.split(":");
  if (!symbol || !pct || Number.isNaN(Number(pct))) throw new Error("SPIKE must look like BTC:-3");
  return { symbol: symbol.toUpperCase(), pct: Number(pct) };
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

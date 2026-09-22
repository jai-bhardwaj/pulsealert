import { Kafka, Partitioners, logLevel, type Producer } from "kafkajs";

/** Topic names. `raw` is written by the Confluent HTTP Source connector,
 *  `ticks`/`moves`/`alerts` are written by Flink SQL, `rules` by this app. */
export const TOPICS = {
  raw: "crypto.spot.raw",
  ticks: "price_ticks",
  moves: "price_moves",
  alerts: "alerts",
  rules: "alert_rules",
} as const;

export const SYMBOLS = ["BTC", "ETH", "SOL"] as const;

function required(key: string): string {
  const v = process.env[key];
  if (!v) throw new Error(`Missing environment variable ${key} (see .env.example)`);
  return v;
}

export function kafka(): Kafka {
  return new Kafka({
    clientId: "pulsealert",
    brokers: [required("KAFKA_BOOTSTRAP")],
    ssl: true,
    sasl: {
      mechanism: "plain",
      username: required("KAFKA_API_KEY"),
      password: required("KAFKA_API_SECRET"),
    },
    logLevel: logLevel.ERROR,
  });
}

/** One shared producer per process (Next.js hot-reload safe). */
const g = globalThis as unknown as { __pulseProducer?: Promise<Producer> };
export function sharedProducer(): Promise<Producer> {
  if (!g.__pulseProducer) {
    const p = kafka().producer({ createPartitioner: Partitioners.DefaultPartitioner });
    g.__pulseProducer = p.connect().then(() => p);
  }
  return g.__pulseProducer;
}

// ---------------------------------------------------------------------------
// Schema Registry helpers (JSON Schema, Confluent wire format:
//   byte 0 = magic 0x00, bytes 1-4 = schema id (big-endian), rest = JSON)
// ---------------------------------------------------------------------------

function srHeaders(): Record<string, string> {
  const token = Buffer.from(
    `${required("SR_API_KEY")}:${required("SR_API_SECRET")}`
  ).toString("base64");
  return {
    Authorization: `Basic ${token}`,
    "Content-Type": "application/vnd.schemaregistry.v1+json",
  };
}

const idCache = new Map<string, number>();

/** Register (or look up) a JSON Schema under `subject`, returning its id. */
export async function registerJsonSchema(subject: string, schema: object): Promise<number> {
  const hit = idCache.get(subject);
  if (hit) return hit;
  const res = await fetch(
    `${required("SR_URL")}/subjects/${encodeURIComponent(subject)}/versions`,
    {
      method: "POST",
      headers: srHeaders(),
      body: JSON.stringify({ schemaType: "JSON", schema: JSON.stringify(schema) }),
    }
  );
  if (!res.ok) throw new Error(`Schema Registry register failed ${res.status}: ${await res.text()}`);
  const { id } = (await res.json()) as { id: number };
  idCache.set(subject, id);
  return id;
}

/** Latest schema id for a subject, or null if the subject does not exist yet. */
export async function latestSchemaId(subject: string): Promise<number | null> {
  const res = await fetch(
    `${required("SR_URL")}/subjects/${encodeURIComponent(subject)}/versions/latest`,
    { headers: srHeaders() }
  );
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`Schema Registry lookup failed ${res.status}: ${await res.text()}`);
  const { id } = (await res.json()) as { id: number };
  return id;
}

export function encodeJsonSR(schemaId: number, value: unknown): Buffer {
  const header = Buffer.alloc(5);
  header.writeUInt8(0, 0);
  header.writeUInt32BE(schemaId, 1);
  return Buffer.concat([header, Buffer.from(JSON.stringify(value), "utf8")]);
}

export function decodeJsonSR<T = unknown>(buf: Buffer | null): T | null {
  if (!buf || buf.length === 0) return null;
  const body = buf[0] === 0 && buf.length > 5 ? buf.subarray(5) : buf; // tolerate plain JSON too
  return JSON.parse(body.toString("utf8")) as T;
}

// ---------------------------------------------------------------------------
// Schemas this app owns
// ---------------------------------------------------------------------------

export type Direction = "DROP" | "RISE";

export interface AlertRule {
  rule_id: string;
  user_id: string;
  symbol: string;
  direction: Direction;
  threshold_pct: number;
  created_at: string;
}

export const RULE_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "AlertRule",
  type: "object",
  properties: {
    rule_id: { type: "string" },
    user_id: { type: "string" },
    symbol: { type: "string" },
    direction: { type: "string", enum: ["DROP", "RISE"] },
    threshold_pct: { type: "number" },
    created_at: { type: "string" },
  },
  required: ["rule_id", "user_id", "symbol", "direction", "threshold_pct", "created_at"],
  additionalProperties: false,
};

/** Same shape the Coinbase spot endpoint returns, used by the fallback producer. */
export const RAW_TICK_SCHEMA = {
  $schema: "http://json-schema.org/draft-07/schema#",
  title: "CoinbaseSpot",
  type: "object",
  properties: {
    data: {
      type: "object",
      properties: {
        amount: { type: "string" },
        base: { type: "string" },
        currency: { type: "string" },
      },
      required: ["amount", "base", "currency"],
    },
  },
  required: ["data"],
};

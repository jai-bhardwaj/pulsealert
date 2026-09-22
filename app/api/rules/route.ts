import { NextResponse } from "next/server";
import { randomUUID } from "node:crypto";
import {
  encodeJsonSR,
  registerJsonSchema,
  sharedProducer,
  RULE_SCHEMA,
  SYMBOLS,
  TOPICS,
  type AlertRule,
} from "@/lib/kafka";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * POST /api/rules  { symbol, direction, threshold_pct, user_id? }
 * Writes the rule to the `alert_rules` Kafka topic (JSON Schema, registered in
 * Schema Registry). Flink SQL joins live price moves against this topic.
 */
export async function POST(req: Request) {
  let body: Partial<AlertRule>;
  try {
    body = await req.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  const symbol = String(body.symbol ?? "").toUpperCase();
  const direction = body.direction === "RISE" ? "RISE" : "DROP";
  const threshold = Number(body.threshold_pct);

  if (!(SYMBOLS as readonly string[]).includes(symbol)) {
    return NextResponse.json({ error: `symbol must be one of ${SYMBOLS.join(", ")}` }, { status: 400 });
  }
  if (!Number.isFinite(threshold) || threshold <= 0 || threshold > 50) {
    return NextResponse.json({ error: "threshold_pct must be between 0 and 50" }, { status: 400 });
  }

  const rule: AlertRule = {
    rule_id: randomUUID(),
    user_id: String(body.user_id ?? "anonymous").slice(0, 64),
    symbol,
    direction,
    threshold_pct: threshold,
    created_at: new Date().toISOString(),
  };

  try {
    const schemaId = await registerJsonSchema(`${TOPICS.rules}-value`, RULE_SCHEMA);
    const producer = await sharedProducer();
    await producer.send({
      topic: TOPICS.rules,
      messages: [{ key: symbol, value: encodeJsonSR(schemaId, rule) }],
    });
    return NextResponse.json({ ok: true, rule });
  } catch (err) {
    console.error("rule publish failed", err);
    return NextResponse.json({ error: (err as Error).message }, { status: 500 });
  }
}

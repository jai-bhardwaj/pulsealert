import { randomUUID } from "node:crypto";
import { decodeJsonSR, kafka, TOPICS } from "@/lib/kafka";

export const runtime = "nodejs";
export const dynamic = "force-dynamic";

/**
 * GET /api/stream  — Server-Sent Events.
 * Tails the Flink-produced topics (`price_ticks`, `price_moves`, `alerts`)
 * and forwards every record to the browser as `event: <topic>`.
 */
export async function GET() {
  const encoder = new TextEncoder();
  const consumer = kafka().consumer({
    groupId: `pulsealert-ui-${randomUUID()}`,
    sessionTimeout: 10_000,
  });

  let heartbeat: NodeJS.Timeout | undefined;

  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      let open = true;
      const send = (event: string, data: unknown) => {
        if (!open) return;
        try {
          controller.enqueue(encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`));
        } catch {
          open = false; // client went away; cancel() will disconnect the consumer
        }
      };

      send("status", { state: "connecting" });
      try {
        await consumer.connect();
        for (const topic of [TOPICS.ticks, TOPICS.moves, TOPICS.alerts]) {
          await consumer.subscribe({ topic, fromBeginning: false });
        }
        await consumer.run({
          eachMessage: async ({ topic, message }) => {
            const value = decodeJsonSR(message.value);
            if (value !== null) send(topic, value);
          },
        });
        send("status", { state: "live" });
        heartbeat = setInterval(() => controller.enqueue(encoder.encode(": ping\n\n")), 15_000);
      } catch (err) {
        send("status", { state: "error", message: (err as Error).message });
        controller.close();
      }
    },
    async cancel() {
      if (heartbeat) clearInterval(heartbeat);
      await consumer.disconnect().catch(() => {});
    },
  });

  return new Response(stream, {
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
    },
  });
}

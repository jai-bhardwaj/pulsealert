import "dotenv/config";
import { kafka, TOPICS } from "../lib/kafka";

/** Creates the two topics that are not created by Flink:
 *  - crypto.spot.raw  (written by the Confluent HTTP Source connector / fallback producer)
 *  - alert_rules      (written by the Next.js app)
 */
async function main() {
  const admin = kafka().admin();
  await admin.connect();
  const existing = new Set(await admin.listTopics());
  const wanted = [TOPICS.raw, TOPICS.rules].filter((t) => !existing.has(t));
  if (wanted.length === 0) {
    console.log("Topics already exist:", TOPICS.raw, TOPICS.rules);
  } else {
    await admin.createTopics({
      waitForLeaders: true,
      topics: wanted.map((topic) => ({ topic, numPartitions: 1, replicationFactor: 3 })),
    });
    console.log("Created topics:", wanted.join(", "));
  }
  await admin.disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});

import { hasCursorSort } from "./filter.js";
import { logger } from "./logger.js";
import { serializeForLog } from "./serialize.js";

export async function verifyCopy({ sourceCollection, targetCollection, config }) {
  const sourceCount = await sourceCollection.countDocuments(config.filter);
  const targetCount = await targetCollection.countDocuments(config.filter);

  let missing = 0;
  let checked = 0;
  let batch = [];

  const cursor = sourceCollection.find(config.filter, {
    projection: { _id: 1 },
    batchSize: config.batchSize,
    ...(hasCursorSort(config.sort) ? { sort: config.sort } : {}),
    ...(config.hint ? { hint: config.hint } : {}),
  });

  try {
    for await (const document of cursor) {
      batch.push(document._id);
      if (batch.length >= config.batchSize) {
        missing += await countMissing(targetCollection, batch);
        checked += batch.length;
        batch = [];
      }
    }
    if (batch.length > 0) {
      missing += await countMissing(targetCollection, batch);
      checked += batch.length;
    }
  } finally {
    await cursor.close().catch(() => undefined);
  }

  const report = {
    sourceCount,
    targetCount,
    checked,
    missing,
    complete: missing === 0,
    extraOnTarget: Math.max(0, targetCount - sourceCount),
  };

  logger.info("verify", serializeForLog(report));
  return report;
}

async function countMissing(targetCollection, ids) {
  const found = await targetCollection.countDocuments({ _id: { $in: ids } });
  return ids.length - found;
}

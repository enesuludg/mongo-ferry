import { appendFailedIds, createCheckpointWindow, pruneFailedIds } from "./checkpoint.js";
import { idKey } from "./ids.js";
import { logger } from "./logger.js";
import { createTaskPool } from "./pool.js";
import { serializeForLog } from "./serialize.js";
import { createBulkWriter } from "./writer.js";

export async function runMigration({
  sourceCollection,
  targetCollection,
  config,
  shouldStop,
}) {
  const stats = createStats();

  if (config.dryRun) {
    return runDryRun(sourceCollection, config, stats);
  }

  const writer = createBulkWriter(targetCollection, {
    onConflict: config.onConflict,
  });
  const pool = createTaskPool(config.concurrency);
  const checkpoint = createCheckpointWindow({
    filePath: config.checkpointFile,
    initialId: config.resumeAfter,
  });
  const cursor = openCursor(sourceCollection, config);
  const state = { fatal: null };
  let nextSeq = 0;
  let batch = [];

  try {
    for await (const document of cursor) {
      if (shouldStop() || state.fatal) {
        logger.warn("stop requested, finishing in-flight batches");
        break;
      }

      batch.push(document);
      if (batch.length >= config.batchSize) {
        const docs = batch;
        batch = [];
        const seq = nextSeq;
        nextSeq += 1;
        await pool.schedule(() => flushBatch({
          seq,
          docs,
          writer,
          config,
          stats,
          checkpoint,
          state,
        }));
      }
    }

    if (batch.length > 0 && !state.fatal) {
      const seq = nextSeq;
      nextSeq += 1;
      await pool.schedule(() => flushBatch({
        seq,
        docs: batch,
        writer,
        config,
        stats,
        checkpoint,
        state,
      }));
    }

    await pool.drain();
    await checkpoint.flush();
    await pruneRetryFile(config, stats);
    stats.lastId = checkpoint.lastCommittedId();
    const snapshot = stats.snapshot();
    snapshot.interrupted = Boolean(shouldStop()) && !state.fatal;
    if (state.fatal) {
      throw state.fatal;
    }
    return snapshot;
  } catch (error) {
    await pool.drain();
    await checkpoint.flush();
    throw error;
  } finally {
    await cursor.close().catch(() => undefined);
  }
}

function openCursor(collection, config) {
  const options = {
    sort: config.sort,
    batchSize: config.batchSize,
    noCursorTimeout: true,
  };

  if (config.projection) {
    options.projection = config.projection;
  }
  if (config.hint) {
    options.hint = config.hint;
  }
  if (config.skip) {
    options.skip = config.skip;
  }
  if (config.limit !== undefined) {
    options.limit = config.limit;
  }

  return collection.find(config.filter, options);
}

async function runDryRun(collection, config, stats) {
  const count = await collection.countDocuments(config.filter);
  const skip = config.skip || 0;
  const sample = await collection
    .find(config.filter, {
      sort: config.sort,
      projection: { _id: 1 },
      skip,
      limit: 5,
      ...(config.hint ? { hint: config.hint } : {}),
    })
    .toArray();

  let wouldCopy = Math.max(0, count - skip);
  if (config.limit !== undefined) {
    wouldCopy = Math.min(wouldCopy, config.limit);
  }
  stats.processed = wouldCopy;
  logger.info("dry-run complete", {
    collection: config.collection,
    matched: count,
    skip,
    wouldCopy,
    sampleIds: sample.map((doc) => String(doc._id)),
    filter: serializeForLog(config.filter),
  });

  return stats.snapshot();
}

async function flushBatch({ seq, docs, writer, config, stats, checkpoint, state }) {
  const lastId = docs[docs.length - 1]._id;

  try {
    const result = await writer.write(docs);
    stats.record(result);

    if (result.failedIds.length > 0) {
      await appendFailedIds(config.failedFile, result.failedIds, "bulkWrite");
      logger.error("batch had failed documents", {
        seq,
        failed: result.failedIds.length,
        succeeded: result.succeeded,
        lastId: String(lastId),
      });
      if (config.stopOnError) {
        state.fatal = Object.assign(
          new Error(`batch ${seq} failed (${result.failedIds.length} document(s))`),
          { failedIds: result.failedIds },
        );
      }
    }

    await checkpoint.report({
      seq,
      lastId,
      ok: result.failedIds.length === 0,
      extra: {
        collection: config.collection,
        processed: stats.processed,
      },
    });
    stats.lastId = checkpoint.lastCommittedId();
    maybeLogProgress(stats, config.collection);
  } catch (error) {
    stats.failed += docs.length;
    stats.failedIds.push(...docs.map((doc) => doc._id));
    await appendFailedIds(config.failedFile, docs.map((doc) => doc._id), error.message);
    await checkpoint.report({
      seq,
      lastId,
      ok: false,
      extra: { collection: config.collection, processed: stats.processed },
    });
    if (config.stopOnError) {
      state.fatal = error;
      return;
    }
    logger.error("batch failed, continuing", {
      seq,
      size: docs.length,
      lastId: String(lastId),
      message: error.message,
    });
  }
}

async function pruneRetryFile(config, stats) {
  if (!config.retryFailedFile || !config.retryIds?.length) {
    return;
  }

  const stillFailed = new Set(stats.failedIds.map(idKey));
  const succeeded = config.retryIds.filter((id) => !stillFailed.has(idKey(id)));
  await pruneFailedIds(config.retryFailedFile, succeeded, config.idType);
}

function createStats() {
  const startedAt = Date.now();

  return {
    processed: 0,
    submitted: 0,
    failed: 0,
    failedIds: [],
    upserted: 0,
    matched: 0,
    modified: 0,
    lastId: null,
    lastLogAt: 0,
    record(result) {
      this.submitted += result.submitted;
      this.processed += result.succeeded;
      this.failed += result.failedIds.length;
      this.failedIds.push(...result.failedIds);
      this.upserted += result.upserted;
      this.matched += result.matched;
      this.modified += result.modified;
    },
    snapshot() {
      const elapsedMs = Date.now() - startedAt;
      const docsPerSecond = elapsedMs === 0 ? 0 : Number((this.processed / (elapsedMs / 1000)).toFixed(1));
      return {
        processed: this.processed,
        submitted: this.submitted,
        failed: this.failed,
        upserted: this.upserted,
        matched: this.matched,
        modified: this.modified,
        lastId: this.lastId ? String(this.lastId) : null,
        elapsedMs,
        docsPerSecond,
      };
    },
  };
}

function maybeLogProgress(stats, collection) {
  const now = Date.now();
  if (now - stats.lastLogAt < 2000) {
    return;
  }

  stats.lastLogAt = now;
  logger.info("progress", { collection, ...stats.snapshot() });
}

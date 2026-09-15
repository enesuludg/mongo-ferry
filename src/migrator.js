import { appendFailedIds, createCheckpointWindow, pruneFailedIds } from "./checkpoint.js";
import { hasCursorSort } from "./filter.js";
import { idKey } from "./ids.js";
import { logger } from "./logger.js";
import { createTaskPool } from "./pool.js";
import { serializeForLog } from "./serialize.js";
import { formatDuration } from "./outcome.js";
import { createBulkWriter } from "./writer.js";

export async function runMigration({
  sourceCollection,
  targetCollection,
  config,
  shouldStop,
}) {
  const stats = createStats();
  const hint = await resolveRuntimeHint(sourceCollection, config);
  const cursorConfig = hint === config.hint ? config : { ...config, hint };

  if (cursorConfig.dryRun) {
    return runDryRun(sourceCollection, cursorConfig, stats);
  }

  const writer = createBulkWriter(targetCollection, {
    onConflict: config.onConflict,
  });
  const pool = createTaskPool(config.concurrency);
  const checkpoint = createCheckpointWindow({
    filePath: config.checkpointFile,
    initialId: config.resumeAfter,
  });
  const retryKeySet = new Set((config.retryIds ?? []).map(idKey));
  logger.info("opening source cursor", {
    collection: cursorConfig.collection,
    batchSize: cursorConfig.batchSize,
    hint: cursorConfig.hint ?? null,
    sort: cursorConfig.sort,
  });
  const cursorOpenedAt = Date.now();
  const cursor = openCursor(sourceCollection, cursorConfig);
  const state = { fatal: null };
  let nextSeq = 0;
  let batch = [];
  let sawFirstDocument = false;

  try {
    for await (const document of cursor) {
      if (!sawFirstDocument) {
        sawFirstDocument = true;
        logger.info("source cursor returned the first document", {
          elapsed: formatDuration(Date.now() - cursorOpenedAt),
          _id: String(document._id),
        });
      }
      if (shouldStop() || state.fatal) {
        logger.warn("stop requested, finishing in-flight batches");
        break;
      }

      batch.push(document);
      if (batch.length >= cursorConfig.batchSize) {
        const docs = batch;
        batch = [];
        const seq = nextSeq;
        nextSeq += 1;
        await pool.schedule(() => flushBatch({
          seq,
          docs,
          writer,
          config: cursorConfig,
          stats,
          checkpoint,
          state,
          retryKeySet,
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
        config: cursorConfig,
        stats,
        checkpoint,
        state,
        retryKeySet,
      }));
    }

    await pool.drain();
    await checkpoint.flush();
    await pruneRetryFile(cursorConfig, stats);
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

async function resolveRuntimeHint(collection, config) {
  if (config.hint) {
    return config.hint;
  }
  if (!config.dateField) {
    return config.hint;
  }

  const discovered = await discoverDateFieldHint(collection, config.dateField);
  if (discovered) {
    logger.info("using source index for date filter", {
      dateField: config.dateField,
      hint: discovered.key,
      name: discovered.name,
    });
    return discovered.key;
  }

  logger.warn("no single-field index found for --dateField; MongoDB 3.6 may collection-scan", {
    dateField: config.dateField,
    batchSize: config.batchSize,
  });
  return undefined;
}

async function discoverDateFieldHint(collection, dateField) {
  if (typeof collection.indexes !== "function") {
    return null;
  }

  let indexes;
  try {
    indexes = await collection.indexes();
  } catch (error) {
    logger.warn("could not list source indexes", { message: error.message });
    return null;
  }

  const singles = (Array.isArray(indexes) ? indexes : []).filter((index) => {
    const keys = index?.key && Object.keys(index.key);
    return keys?.length === 1 && keys[0] === dateField;
  });
  if (singles.length === 0) {
    return null;
  }

  const descending = singles.find((index) => index.key[dateField] === -1);
  const chosen = descending ?? singles[0];
  return { key: chosen.key, name: chosen.name };
}

function openCursor(collection, config) {
  const options = {
    batchSize: config.batchSize,
    noCursorTimeout: true,
  };

  if (hasCursorSort(config.sort)) {
    options.sort = config.sort;
  }
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
      projection: { _id: 1 },
      skip,
      limit: 5,
      ...(hasCursorSort(config.sort) ? { sort: config.sort } : {}),
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

async function flushBatch({ seq, docs, writer, config, stats, checkpoint, state, retryKeySet }) {
  const lastId = docs[docs.length - 1]._id;

  try {
    const result = await writer.write(docs);
    stats.record(result, retryKeySet, docs);
    const failedIds = result.failedIds;
    let recorded = failedIds.length === 0;

    if (failedIds.length > 0) {
      try {
        await appendFailedIds(config.failedFile, failedIds, "bulkWrite");
        recorded = true;
      } catch (error) {
        logger.error("could not persist failed ids; checkpoint will not advance past this batch", {
          seq,
          message: error.message,
        });
        state.fatal = Object.assign(
          new Error(`failed-id append failed for batch ${seq}: ${error.message}`),
          { failedIds, cause: error },
        );
      }

      logger.error("batch had failed documents", {
        seq,
        failed: failedIds.length,
        succeeded: result.succeeded,
        lastId: String(lastId),
        recorded,
      });
      if (recorded && config.stopOnError && !state.fatal) {
        state.fatal = Object.assign(
          new Error(`batch ${seq} failed (${failedIds.length} document(s))`),
          { failedIds },
        );
      }
    }

    await checkpoint.report({
      seq,
      lastId,
      ok: failedIds.length === 0,
      recorded,
      extra: {
        collection: config.collection,
        processed: stats.processed,
      },
    });
    stats.lastId = checkpoint.lastCommittedId();
    maybeLogProgress(stats, config.collection);
  } catch (error) {
    const failedIds = docs.map((doc) => doc._id);
    stats.failed += docs.length;
    stats.noteAttemptedIds(failedIds, retryKeySet);
    stats.noteFailedIds(failedIds, retryKeySet);
    let recorded = false;
    try {
      await appendFailedIds(config.failedFile, failedIds, error.message);
      recorded = true;
    } catch (appendError) {
      logger.error("could not persist failed ids; checkpoint will not advance past this batch", {
        seq,
        message: appendError.message,
      });
      state.fatal = Object.assign(
        new Error(`failed-id append failed for batch ${seq}: ${appendError.message}`),
        { failedIds, cause: appendError },
      );
    }

    await checkpoint.report({
      seq,
      lastId,
      ok: false,
      recorded,
      extra: { collection: config.collection, processed: stats.processed },
    });
    if (!recorded) {
      return;
    }
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

  const succeeded = config.retryIds.filter((id) => {
    const key = idKey(id);
    return stats.attemptedRetryKeys.has(key) && !stats.failedRetryKeys.has(key);
  });
  await pruneFailedIds(config.retryFailedFile, succeeded, config.idType);
}

function createStats() {
  const startedAt = Date.now();

  return {
    processed: 0,
    submitted: 0,
    failed: 0,
    failedRetryKeys: new Set(),
    attemptedRetryKeys: new Set(),
    upserted: 0,
    matched: 0,
    modified: 0,
    lastId: null,
    lastLogAt: 0,
    record(result, retryKeySet, docs) {
      this.submitted += result.submitted;
      this.processed += result.succeeded;
      this.failed += result.failedIds.length;
      this.noteAttemptedIds(docs.map((doc) => doc._id), retryKeySet);
      this.noteFailedIds(result.failedIds, retryKeySet);
      this.upserted += result.upserted;
      this.matched += result.matched;
      this.modified += result.modified;
    },
    noteAttemptedIds(ids, retryKeySet) {
      addRetryKeys(this.attemptedRetryKeys, ids, retryKeySet);
    },
    noteFailedIds(ids, retryKeySet) {
      addRetryKeys(this.failedRetryKeys, ids, retryKeySet);
    },
    snapshot() {
      const elapsedMs = Date.now() - startedAt;
      const docsPerSecond = elapsedMs === 0 ? 0 : Number((this.processed / (elapsedMs / 1000)).toFixed(1));
      return {
        processed: this.processed,
        submitted: this.submitted,
        failed: this.failed,
        created: this.upserted,
        updated: this.matched,
        lastId: this.lastId ? String(this.lastId) : null,
        elapsedMs,
        elapsed: formatDuration(elapsedMs),
        docsPerSecond,
      };
    },
  };
}

function addRetryKeys(target, ids, retryKeySet) {
  if (!retryKeySet || retryKeySet.size === 0) {
    return;
  }
  for (const id of ids) {
    const key = idKey(id);
    if (retryKeySet.has(key)) {
      target.add(key);
    }
  }
}

function maybeLogProgress(stats, collection) {
  const now = Date.now();
  if (now - stats.lastLogAt < 2000) {
    return;
  }

  stats.lastLogAt = now;
  logger.info("progress", { collection, ...stats.snapshot() });
}

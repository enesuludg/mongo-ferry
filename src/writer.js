const RETRY_DELAYS_MS = [250, 1000, 3000];

const PERMANENT_ERROR_CODES = new Set([
  2, 13, 14, 18, 22, 28, 33, 121, 125, 167, 10334, 11000, 11001,
]);

const RETRYABLE_ERROR_CODES = new Set([
  6, 7, 50, 64, 89, 91, 112, 133, 189, 202, 262, 9001, 10107, 11600, 11602, 13435, 13436, 16500,
]);

export function createBulkWriter(collection, options = {}) {
  const {
    retryDelaysMs = RETRY_DELAYS_MS,
    onConflict = "replace",
  } = options;

  return {
    async write(docs) {
      if (docs.length === 0) {
        return emptyResult(0);
      }

      const totals = emptyResult(docs.length);
      let pending = docs;
      const failedIds = [];

      for (let attempt = 0; ; attempt += 1) {
        try {
          const result = await collection.bulkWrite(buildWriteOperations(pending, onConflict), {
            ordered: false,
          });
          addCounts(totals, summarizeServerResult(result));
          totals.succeeded += pending.length;
          totals.failedIds = failedIds;
          return totals;
        } catch (error) {
          const classified = classifyBulkError(error, pending);
          addCounts(totals, classified.result);
          totals.succeeded += classified.succeededCount;

          for (const doc of classified.permanentFailed) {
            failedIds.push(doc._id);
          }

          const canRetry = classified.retryableFailed.length > 0 && attempt < retryDelaysMs.length;
          if (!canRetry) {
            for (const doc of classified.retryableFailed) {
              failedIds.push(doc._id);
            }
            totals.failedIds = failedIds;
            return totals;
          }

          pending = classified.retryableFailed;
          await sleep(retryDelaysMs[attempt]);
        }
      }
    },
  };
}

export function buildWriteOperations(docs, onConflict = "replace") {
  if (onConflict === "skip") {
    return docs.map((doc) => ({
      updateOne: {
        filter: { _id: doc._id },
        update: { $setOnInsert: doc },
        upsert: true,
      },
    }));
  }

  if (onConflict === "merge") {
    return docs.map((doc) => {
      const rest = { ...doc };
      delete rest._id;
      return {
        updateOne: {
          filter: { _id: doc._id },
          update: { $set: rest },
          upsert: true,
        },
      };
    });
  }

  return docs.map((doc) => ({
    replaceOne: {
      filter: { _id: doc._id },
      replacement: doc,
      upsert: true,
    },
  }));
}

export function classifyBulkError(error, docs) {
  const writeErrors = normalizeWriteErrors(error);

  if (writeErrors.length === 0) {
    if (isRetryableWriteError(error)) {
      return {
        retryableFailed: docs,
        permanentFailed: [],
        succeededCount: 0,
        result: emptyCounts(),
      };
    }
    return {
      retryableFailed: [],
      permanentFailed: docs,
      succeededCount: 0,
      result: emptyCounts(),
    };
  }

  const retryableFailed = [];
  const permanentFailed = [];
  const failedIndexes = new Set();

  for (const writeError of writeErrors) {
    const index = writeError.index;
    failedIndexes.add(index);
    const doc = docs[index];
    if (!doc) {
      continue;
    }
    if (isRetryableWriteError(writeError)) {
      retryableFailed.push(doc);
    } else {
      permanentFailed.push(doc);
    }
  }

  return {
    retryableFailed,
    permanentFailed,
    succeededCount: docs.length - failedIndexes.size,
    result: summarizeServerResult(error),
  };
}

export function isRetryableWriteError(error) {
  if (!error || typeof error !== "object") {
    return false;
  }
  if (typeof error.hasErrorLabel === "function") {
    if (error.hasErrorLabel("RetryableWriteError") || error.hasErrorLabel("ResetPool")) {
      return true;
    }
  }
  if (PERMANENT_ERROR_CODES.has(error.code)) {
    return false;
  }
  if (RETRYABLE_ERROR_CODES.has(error.code)) {
    return true;
  }
  const name = error.name || "";
  return name === "MongoNetworkError" || name === "MongoTimeoutError" || name === "MongoServerSelectionError";
}

function normalizeWriteErrors(error) {
  const raw = error.writeErrors ?? error.result?.getWriteErrors?.() ?? [];
  return Array.isArray(raw) ? raw : [raw];
}

function summarizeServerResult(result) {
  const source = result?.result && typeof result.result === "object" ? result.result : result;
  return {
    upserted: source?.upsertedCount ?? source?.nUpserted ?? 0,
    matched: source?.matchedCount ?? source?.nMatched ?? 0,
    modified: source?.modifiedCount ?? source?.nModified ?? 0,
  };
}

function addCounts(totals, counts) {
  totals.upserted += counts.upserted;
  totals.matched += counts.matched;
  totals.modified += counts.modified;
}

function emptyResult(submitted) {
  return {
    submitted,
    succeeded: 0,
    upserted: 0,
    matched: 0,
    modified: 0,
    failedIds: [],
  };
}

function emptyCounts() {
  return { upserted: 0, matched: 0, modified: 0 };
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

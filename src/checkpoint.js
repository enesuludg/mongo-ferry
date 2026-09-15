import { appendFile, readFile, writeFile } from "node:fs/promises";
import { describeId, idKey, restoreId } from "./ids.js";
import { logger } from "./logger.js";

const appendChains = new Map();

export async function readCheckpoint(filePath, idType = "auto") {
  try {
    const raw = await readFile(filePath, "utf8");
    const parsed = JSON.parse(raw);
    if (parsed?.lastId === undefined || parsed?.lastId === null || parsed?.lastId === "") {
      return null;
    }
    return restoreId({ value: parsed.lastId, type: parsed.lastIdType }, idType);
  } catch (error) {
    if (error.code === "ENOENT") {
      return null;
    }
    throw error;
  }
}

export async function writeCheckpoint(filePath, lastId, extra = {}) {
  const described = describeId(lastId);
  const payload = {
    lastId: described.value,
    lastIdType: described.type,
    updatedAt: new Date().toISOString(),
    ...extra,
  };

  await writeFile(filePath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

export function appendFailedIds(filePath, ids, reason) {
  if (!ids.length) {
    return Promise.resolve();
  }

  const previous = appendChains.get(filePath) ?? Promise.resolve();
  const next = previous.catch(() => undefined).then(async () => {
    const lines = ids
      .map((id) => {
        const described = describeId(id);
        return JSON.stringify({
          _id: described.value,
          _idType: described.type,
          reason,
          at: new Date().toISOString(),
        });
      })
      .join("\n");
    await appendFile(filePath, `${lines}\n`, "utf8");
    logger.warn(`wrote ${ids.length} failed _id(s) to ${filePath}`);
  });

  const tracked = next.catch(() => undefined);
  appendChains.set(filePath, tracked);
  tracked.finally(() => {
    if (appendChains.get(filePath) === tracked) {
      appendChains.delete(filePath);
    }
  });

  return next.catch((error) => {
    logger.error("failed-id append failed", { message: error.message, filePath });
    throw error;
  });
}

export async function readFailedIds(filePath, idType = "auto") {
  const records = await readFailedIdRecords(filePath, idType);
  const seen = new Set();
  const ids = [];

  for (const record of records) {
    const key = idKey(record.id);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    ids.push(record.id);
  }

  return ids;
}

export async function pruneFailedIds(filePath, idsToRemove, idType = "auto") {
  if (!idsToRemove.length) {
    return;
  }

  const removeKeys = new Set(idsToRemove.map(idKey));
  const remaining = [];

  try {
    const records = await readFailedIdRecords(filePath, idType);
    for (const record of records) {
      if (removeKeys.has(idKey(record.id))) {
        continue;
      }
      remaining.push(record.raw);
    }
  } catch (error) {
    if (error.code === "ENOENT") {
      return;
    }
    throw error;
  }

  const body = remaining.length === 0 ? "" : `${remaining.map((row) => JSON.stringify(row)).join("\n")}\n`;
  await writeFile(filePath, body, "utf8");
}

async function readFailedIdRecords(filePath, idType = "auto") {
  const raw = await readFile(filePath, "utf8");
  const records = [];

  for (const line of raw.split("\n")) {
    const trimmed = line.trim();
    if (!trimmed) {
      continue;
    }
    const parsed = JSON.parse(trimmed);
    if (parsed?._id === undefined || parsed?._id === null || parsed?._id === "") {
      continue;
    }
    records.push({
      id: restoreId({ value: parsed._id, type: parsed._idType }, idType),
      raw: parsed,
    });
  }

  return records;
}

export function createCheckpointWindow({ filePath, initialId = null, write = writeCheckpoint }) {
  let nextExpected = 0;
  const succeeded = new Map();
  const failed = new Set();
  let lastCommittedId = initialId;
  let chain = Promise.resolve();

  return {
    lastCommittedId: () => lastCommittedId,
    report({ seq, lastId, ok, recorded = true, extra = {} }) {
      chain = chain
        .then(async () => {
          if (ok) {
            succeeded.set(seq, lastId);
          } else if (recorded) {
            failed.add(seq);
          }

          let slid = false;
          while (succeeded.has(nextExpected) || failed.has(nextExpected)) {
            if (succeeded.has(nextExpected)) {
              lastCommittedId = succeeded.get(nextExpected);
              succeeded.delete(nextExpected);
            } else {
              failed.delete(nextExpected);
            }
            nextExpected += 1;
            slid = true;
          }

          if (slid && lastCommittedId !== undefined && lastCommittedId !== null) {
            await write(filePath, lastCommittedId, {
              ...extra,
              contiguousSeq: nextExpected,
            });
          }
        })
        .catch((error) => {
          logger.warn("checkpoint write failed", { message: error.message });
        });
      return chain;
    },
    flush() {
      return chain;
    },
  };
}

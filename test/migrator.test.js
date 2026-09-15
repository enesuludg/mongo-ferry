import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ObjectId } from "mongodb";
import { runMigration } from "../src/migrator.js";

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function memorySource(docs) {
  return {
    find() {
      return {
        async *[Symbol.asyncIterator]() {
          for (const doc of docs) {
            yield doc;
          }
        },
        async close() {},
      };
    },
    async countDocuments() {
      return docs.length;
    },
  };
}

async function tempDir() {
  return mkdtemp(join(tmpdir(), "mongo-migrate-"));
}

test("runMigration upserts cursor documents in batches", async () => {
  const docs = [
    { _id: new ObjectId(), name: "one" },
    { _id: new ObjectId(), name: "two" },
    { _id: new ObjectId(), name: "three" },
  ];
  const writes = [];
  const directory = await tempDir();
  const checkpointFile = join(directory, "checkpoint.json");

  const result = await runMigration({
    sourceCollection: memorySource(docs),
    targetCollection: {
      async bulkWrite(operations) {
        writes.push(operations);
        return { upsertedCount: operations.length, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: { _id: { $gte: new ObjectId() } },
      sort: { _id: 1 },
      batchSize: 2,
      concurrency: 2,
      dryRun: false,
      stopOnError: true,
      onConflict: "replace",
      checkpointFile,
      failedFile: join(directory, "failed.jsonl"),
    },
    shouldStop: () => false,
  });

  assert.equal(result.processed, 3);
  assert.equal(writes.length, 2);
  const sizes = writes.map((write) => write.length).sort((left, right) => left - right);
  assert.deepEqual(sizes, [1, 2]);
  assert.ok(writes.some((write) => write[0].replaceOne?.upsert));

  const checkpoint = JSON.parse(await readFile(checkpointFile, "utf8"));
  assert.equal(checkpoint.lastId, String(docs[2]._id));
});

test("migrator keeps multiple bulkWrites in flight", async () => {
  const docs = Array.from({ length: 10 }, (_item, index) => ({ _id: index + 1 }));
  let current = 0;
  let peak = 0;
  const directory = await tempDir();
  const started = Date.now();

  await runMigration({
    sourceCollection: memorySource(docs),
    targetCollection: {
      async bulkWrite() {
        current += 1;
        peak = Math.max(peak, current);
        await sleep(50);
        current -= 1;
        return { upsertedCount: 1, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: {},
      sort: { _id: 1 },
      batchSize: 1,
      concurrency: 8,
      dryRun: false,
      stopOnError: false,
      onConflict: "replace",
      checkpointFile: join(directory, "checkpoint.json"),
      failedFile: join(directory, "failed.jsonl"),
    },
    shouldStop: () => false,
  });

  const wallClockMs = Date.now() - started;
  assert.equal(peak, 8);
  assert.ok(wallClockMs < 250, `expected parallel writes, got ${wallClockMs}ms with peak ${peak}`);
});

test("checkpoint advances past a failed batch after recording failed ids", async () => {
  const docs = Array.from({ length: 6 }, (_item, index) => ({
    _id: new ObjectId(`6553f100000000000000000${index}`),
  }));
  const directory = await tempDir();
  const checkpointFile = join(directory, "checkpoint.json");
  const failedFile = join(directory, "failed.jsonl");

  const result = await runMigration({
    sourceCollection: memorySource(docs),
    targetCollection: {
      async bulkWrite(operations) {
        const firstId = String(operations[0].replaceOne.filter._id);
        if (firstId === String(docs[0]._id)) {
          const error = new Error("validation");
          error.code = 121;
          error.writeErrors = operations.map((_op, index) => ({ index, code: 121 }));
          throw error;
        }
        return { upsertedCount: operations.length, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: {},
      sort: { _id: 1 },
      batchSize: 2,
      concurrency: 3,
      dryRun: false,
      stopOnError: false,
      onConflict: "replace",
      checkpointFile,
      failedFile,
    },
    shouldStop: () => false,
  });

  assert.equal(result.failed, 2);
  const checkpoint = JSON.parse(await readFile(checkpointFile, "utf8"));
  assert.equal(checkpoint.lastId, String(docs[5]._id));
  const failed = await readFile(failedFile, "utf8");
  assert.ok(failed.includes(String(docs[0]._id)));
  assert.ok(failed.includes(String(docs[1]._id)));
});

test("dryRun uses countDocuments instead of scanning every document", async () => {
  let scanned = 0;
  const sourceCollection = {
    async countDocuments() {
      return 42;
    },
    find() {
      return {
        async toArray() {
          return [{ _id: "sample" }];
        },
      };
    },
  };

  const result = await runMigration({
    sourceCollection,
    targetCollection: {
      async bulkWrite() {
        scanned += 1;
        return { upsertedCount: 0, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: {},
      sort: { _id: 1 },
      dryRun: true,
      skip: 10,
      batchSize: 1000,
      concurrency: 1,
    },
    shouldStop: () => false,
  });

  assert.equal(result.processed, 32);
  assert.equal(scanned, 0);
});

test("stopOnError rejects cleanly without unhandledRejection and closes the cursor", async () => {
  const rejections = [];
  const onUnhandled = (error) => {
    rejections.push(error);
  };
  process.on("unhandledRejection", onUnhandled);

  let closed = 0;
  const docs = [{ _id: 1 }, { _id: 2 }, { _id: 3 }];
  const directory = await tempDir();

  try {
    await assert.rejects(
      () => runMigration({
        sourceCollection: {
          find() {
            return {
              async *[Symbol.asyncIterator]() {
                for (const doc of docs) {
                  yield doc;
                }
              },
              async close() {
                closed += 1;
              },
            };
          },
        },
        targetCollection: {
          async bulkWrite() {
            const error = new Error("validation");
            error.code = 121;
            error.writeErrors = [{ index: 0, code: 121 }];
            throw error;
          },
        },
        config: {
          collection: "users",
          filter: {},
          sort: { _id: 1 },
          batchSize: 1,
          concurrency: 3,
          dryRun: false,
          stopOnError: true,
          onConflict: "replace",
          checkpointFile: join(directory, "checkpoint.json"),
          failedFile: join(directory, "failed.jsonl"),
        },
        shouldStop: () => false,
      }),
      /batch \d+ failed/,
    );
    await sleep(20);
    assert.equal(rejections.length, 0);
    assert.equal(closed, 1);
  } finally {
    process.off("unhandledRejection", onUnhandled);
  }
});

test("runMigration marks interrupted when stop is requested", async () => {
  const directory = await tempDir();
  const result = await runMigration({
    sourceCollection: memorySource([{ _id: 1 }, { _id: 2 }]),
    targetCollection: {
      async bulkWrite() {
        return { upsertedCount: 1, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: {},
      sort: { _id: 1 },
      batchSize: 1,
      concurrency: 1,
      dryRun: false,
      stopOnError: false,
      onConflict: "replace",
      checkpointFile: join(directory, "checkpoint.json"),
      failedFile: join(directory, "failed.jsonl"),
    },
    shouldStop: () => true,
  });

  assert.equal(result.interrupted, true);
  assert.equal(result.processed, 0);
});

test("retryFailed prunes successful ids from the file", async () => {
  const directory = await tempDir();
  const retryFile = join(directory, "failed-ids.jsonl");
  await writeFile(
    retryFile,
    `${JSON.stringify({ _id: 1, _idType: "number" })}\n${JSON.stringify({ _id: 2, _idType: "number" })}\n`,
    "utf8",
  );

  await runMigration({
    sourceCollection: memorySource([{ _id: 1 }, { _id: 2 }]),
    targetCollection: {
      async bulkWrite() {
        return { upsertedCount: 2, matchedCount: 0, modifiedCount: 0 };
      },
    },
    config: {
      collection: "users",
      filter: { _id: { $in: [1, 2] } },
      sort: { _id: 1 },
      batchSize: 10,
      concurrency: 1,
      dryRun: false,
      stopOnError: false,
      onConflict: "replace",
      checkpointFile: join(directory, "checkpoint.json"),
      failedFile: join(directory, "other-failed.jsonl"),
      retryFailedFile: retryFile,
      retryIds: [1, 2],
    },
    shouldStop: () => false,
  });

  const remaining = await readFile(retryFile, "utf8");
  assert.equal(remaining, "");
});

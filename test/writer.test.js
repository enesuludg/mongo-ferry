import assert from "node:assert/strict";
import test from "node:test";
import { ObjectId } from "mongodb";
import {
  buildWriteOperations,
  classifyBulkError,
  createBulkWriter,
  isRetryableWriteError,
} from "../src/writer.js";

test("createBulkWriter sends unordered replaceOne upserts", async () => {
  const ops = [];
  const collection = {
    async bulkWrite(operations, options) {
      ops.push({ operations, options });
      return { upsertedCount: 2, matchedCount: 0, modifiedCount: 0 };
    },
  };

  const docs = [{ _id: new ObjectId(), name: "a" }, { _id: new ObjectId(), name: "b" }];
  const writer = createBulkWriter(collection);
  const result = await writer.write(docs);

  assert.equal(result.submitted, 2);
  assert.equal(result.succeeded, 2);
  assert.equal(result.upserted, 2);
  assert.equal(ops[0].options.ordered, false);
  assert.equal(ops[0].operations[0].replaceOne.upsert, true);
  assert.deepEqual(ops[0].operations[0].replaceOne.replacement, docs[0]);
});

test("createBulkWriter retries only retryable docs then returns failed ids", async () => {
  let attempts = 0;
  const collection = {
    async bulkWrite() {
      attempts += 1;
      const error = new Error("transient");
      error.name = "MongoNetworkError";
      throw error;
    },
  };

  const docs = [{ _id: new ObjectId() }];
  const writer = createBulkWriter(collection, { retryDelaysMs: [1, 1, 1] });
  const result = await writer.write(docs);

  assert.equal(result.failedIds.length, 1);
  assert.equal(result.succeeded, 0);
  assert.equal(attempts, 4);
});

test("TypeError is not retried", async () => {
  let attempts = 0;
  const collection = {
    async bulkWrite() {
      attempts += 1;
      throw new TypeError("docs.map is not a function");
    },
  };

  const result = await createBulkWriter(collection, { retryDelaysMs: [50, 50, 50] }).write([{ _id: 1 }]);
  assert.equal(attempts, 1);
  assert.deepEqual(result.failedIds, [1]);
});

test("permanent errors are not retried", async () => {
  let attempts = 0;
  const collection = {
    async bulkWrite() {
      attempts += 1;
      const error = new Error("unauthorized");
      error.code = 13;
      throw error;
    },
  };

  const writer = createBulkWriter(collection, { retryDelaysMs: [50, 50, 50] });
  const result = await writer.write([{ _id: 1 }]);
  assert.equal(attempts, 1);
  assert.deepEqual(result.failedIds, [1]);
});

test("partial bulkWrite errors fail only the errored indexes", async () => {
  const docs = [{ _id: 1 }, { _id: 2 }, { _id: 3 }];
  const collection = {
    async bulkWrite() {
      const error = new Error("bulk write error");
      error.writeErrors = [{ index: 1, code: 121, errmsg: "validation" }];
      error.upsertedCount = 1;
      error.matchedCount = 1;
      error.modifiedCount = 1;
      throw error;
    },
  };

  const result = await createBulkWriter(collection).write(docs);
  assert.equal(result.succeeded, 2);
  assert.deepEqual(result.failedIds, [2]);
});

test("onConflict skip and merge build different operations", () => {
  const docs = [{ _id: "user_abc", name: "Ada", extra: 1 }];
  assert.equal(buildWriteOperations(docs, "skip")[0].updateOne.update.$setOnInsert.name, "Ada");
  assert.deepEqual(buildWriteOperations(docs, "merge")[0].updateOne.update.$set, { name: "Ada", extra: 1 });
  assert.equal(buildWriteOperations(docs, "replace")[0].replaceOne.upsert, true);
});

test("classifyBulkError and retryable detection", () => {
  assert.equal(isRetryableWriteError({ code: 89 }), true);
  assert.equal(isRetryableWriteError({ code: 121 }), false);
  assert.equal(isRetryableWriteError({ name: "MongoNetworkError" }), true);
  assert.equal(isRetryableWriteError(new TypeError("boom")), false);
  assert.equal(isRetryableWriteError(new Error("no code")), false);

  const classified = classifyBulkError(
    { writeErrors: [{ index: 0, code: 11000 }], upsertedCount: 1 },
    [{ _id: "a" }, { _id: "b" }],
  );
  assert.equal(classified.succeededCount, 1);
  assert.equal(classified.permanentFailed[0]._id, "a");
});

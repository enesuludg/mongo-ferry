import assert from "node:assert/strict";
import test from "node:test";
import { verifyCopy } from "../src/verify.js";

test("verifyCopy reports missing target documents", async () => {
  const sourceIds = [{ _id: 1 }, { _id: 2 }, { _id: 3 }];
  const targetIds = new Set([1, 3]);

  const report = await verifyCopy({
    sourceCollection: {
      async countDocuments() {
        return 3;
      },
      find() {
        return {
          async *[Symbol.asyncIterator]() {
            for (const doc of sourceIds) {
              yield doc;
            }
          },
          async close() {},
        };
      },
    },
    targetCollection: {
      async countDocuments(filter) {
        if (filter._id?.$in) {
          return filter._id.$in.filter((id) => targetIds.has(id)).length;
        }
        return 2;
      },
    },
    config: {
      filter: {},
      sort: { _id: 1 },
      batchSize: 2,
    },
  });

  assert.equal(report.sourceCount, 3);
  assert.equal(report.targetCount, 2);
  assert.equal(report.checked, 3);
  assert.equal(report.missing, 1);
  assert.equal(report.complete, false);
});

test("verifyCopy is complete when every source id exists even if target has extras", async () => {
  const sourceIds = [{ _id: 1 }, { _id: 2 }, { _id: 3 }];
  const targetIds = new Set([1, 2, 3, 4, 5]);

  const report = await verifyCopy({
    sourceCollection: {
      async countDocuments() {
        return 3;
      },
      find() {
        return {
          async *[Symbol.asyncIterator]() {
            for (const doc of sourceIds) {
              yield doc;
            }
          },
          async close() {},
        };
      },
    },
    targetCollection: {
      async countDocuments(filter) {
        if (filter._id?.$in) {
          return filter._id.$in.filter((id) => targetIds.has(id)).length;
        }
        return 5;
      },
    },
    config: {
      filter: {},
      sort: { _id: 1 },
      batchSize: 10,
    },
  });

  assert.equal(report.sourceCount, 3);
  assert.equal(report.targetCount, 5);
  assert.equal(report.missing, 0);
  assert.equal(report.complete, true);
  assert.equal(report.extraOnTarget, 2);
});

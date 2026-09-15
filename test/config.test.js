import assert from "node:assert/strict";
import test from "node:test";
import { ObjectId } from "mongodb";
import { loadConfig } from "../src/config.js";

const URIS = {
  SOURCE_URI: "mongodb://localhost:27017/sourceDb",
  TARGET_URI: "mongodb://localhost:27017/targetDb",
};

test("loadConfig copies the whole collection when --since is omitted", async () => {
  const config = await loadConfig({}, URIS);

  assert.equal(config.collection, "users");
  assert.equal(config.targetCollection, "users");
  assert.equal(config.sourceDb, "sourceDb");
  assert.equal(config.targetDb, "targetDb");
  assert.equal(config.usedDefaultDateWindow, false);
  assert.equal(config.usedObjectIdWindow, false);
  assert.equal(config.migrateAll, true);
  assert.deepEqual(config.filter, {});
  assert.deepEqual(config.hint, { _id: 1 });
  assert.deepEqual(config.sort, { _id: 1 });
  assert.equal(config.batchSize, 1000);
  assert.equal(config.concurrency, 16);
  assert.equal(config.targetPoolSize, 64);
  assert.equal(config.onConflict, "replace");
});

test("loadConfig env SINCE does not apply a window without --since", async () => {
  const config = await loadConfig({}, { ...URIS, SINCE: "7d" });
  assert.equal(config.migrateAll, true);
  assert.deepEqual(config.filter, {});
});

test("loadConfig --since applies an ObjectId time window", async () => {
  const config = await loadConfig({ since: "15d" }, URIS);
  assert.equal(config.usedDefaultDateWindow, true);
  assert.equal(config.usedObjectIdWindow, true);
  assert.ok(config.filter._id.$gte instanceof ObjectId);
});

test("loadConfig prefers explicit query over the default date window", async () => {
  const config = await loadConfig(
    {
      collection: "user",
      targetCollection: "users_v7",
      query: '{"status":"active"}',
      concurrency: "32",
    },
    {
      SOURCE_URI: "mongodb://localhost:27017/app",
      TARGET_URI: "mongodb://localhost:27017/app",
    },
  );

  assert.equal(config.collection, "user");
  assert.equal(config.targetCollection, "users_v7");
  assert.deepEqual(config.filter, { status: "active" });
  assert.equal(config.usedDefaultDateWindow, false);
  assert.equal(config.concurrency, 32);
  assert.equal(config.hint, undefined);
});

test("loadConfig uses a Date field only when --dateField is set", async () => {
  const config = await loadConfig({ dateField: "createdAt" }, URIS);
  assert.ok(config.filter.createdAt.$gte instanceof Date);
  assert.equal(config.usedObjectIdWindow, false);
  assert.equal(config.sort, null);
  assert.equal(config.hint, undefined);
});

test("loadConfig dateField stays unsorted unless --sort is set", async () => {
  const unsorted = await loadConfig({ dateField: "updatedAt", since: "15d" }, URIS);
  assert.equal(unsorted.sort, null);
  assert.equal(unsorted.hint, undefined);

  const byUpdatedAt = await loadConfig(
    { dateField: "updatedAt", sort: '{"updatedAt":1}' },
    URIS,
  );
  assert.deepEqual(byUpdatedAt.sort, { updatedAt: 1 });
  assert.equal(byUpdatedAt.hint, undefined);

  const byId = await loadConfig(
    { dateField: "updatedAt", sort: '{"_id":1}' },
    URIS,
  );
  assert.deepEqual(byId.sort, { _id: 1 });
  assert.equal(byId.hint, undefined);

  const explicitHint = await loadConfig(
    { dateField: "updatedAt", hint: '{"updatedAt":-1}' },
    URIS,
  );
  assert.deepEqual(explicitHint.hint, { updatedAt: -1 });
});

test("loadConfig sort none disables cursor sort", async () => {
  const config = await loadConfig({ sort: "none" }, URIS);
  assert.equal(config.sort, null);
  assert.equal(config.hint, undefined);
});

test("loadConfig rejects resume when sort is not {_id:1}", async () => {
  await assert.rejects(
    () => loadConfig({ resume: true, sort: '{"createdAt":1}' }, URIS),
    /require the default sort/,
  );
  await assert.rejects(
    () => loadConfig({ resume: true, dateField: "updatedAt" }, URIS),
    /require the default sort/,
  );
  await assert.rejects(
    () => loadConfig({ resume: true, sort: "none" }, URIS),
    /require the default sort/,
  );
});

test("loadConfig accepts string and numeric ids", async () => {
  const config = await loadConfig({ ids: "user_abc,1001" }, URIS);
  assert.deepEqual(config.filter._id.$in, ["user_abc", 1001]);
});

test("loadConfig retryFailed loads ids from jsonl", async () => {
  const { mkdtemp, writeFile } = await import("node:fs/promises");
  const { tmpdir } = await import("node:os");
  const { join } = await import("node:path");
  const filePath = join(await mkdtemp(join(tmpdir(), "retry-")), "failed-ids.jsonl");
  await writeFile(filePath, `${JSON.stringify({ _id: "user_abc" })}\n`, "utf8");

  const config = await loadConfig({ retryFailed: filePath }, URIS);
  assert.deepEqual(config.filter._id.$in, ["user_abc"]);
});

test("loadConfig rejects skip combined with resume", async () => {
  await assert.rejects(
    () => loadConfig({ resume: true, skip: "10" }, URIS),
    /--skip cannot be combined/,
  );
});

test("loadConfig idType string keeps numeric ids as strings", async () => {
  const config = await loadConfig({ ids: "1001", idType: "string" }, URIS);
  assert.deepEqual(config.filter._id.$in, ["1001"]);
});

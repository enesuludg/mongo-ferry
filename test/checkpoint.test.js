import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { appendFailedIds, createCheckpointWindow, pruneFailedIds, readFailedIds } from "../src/checkpoint.js";

async function tempDir() {
  return mkdtemp(join(tmpdir(), "mongo-checkpoint-"));
}

test("checkpoint window does not slide over an unrecorded failure", async () => {
  const directory = await tempDir();
  const filePath = join(directory, "checkpoint.json");
  const window = createCheckpointWindow({ filePath });

  await window.report({ seq: 1, lastId: "b", ok: true });
  await window.report({ seq: 2, lastId: "c", ok: true });
  await window.report({ seq: 0, lastId: "a", ok: false, recorded: false });
  await window.flush();

  await assert.rejects(() => readFile(filePath), { code: "ENOENT" });
  assert.equal(window.lastCommittedId(), null);
});

test("checkpoint window advances past recorded failed seqs", async () => {
  const directory = await tempDir();
  const filePath = join(directory, "checkpoint.json");
  const window = createCheckpointWindow({ filePath });

  await window.report({ seq: 1, lastId: "b", ok: true });
  await window.report({ seq: 2, lastId: "c", ok: true });
  await window.report({ seq: 0, lastId: "a", ok: false });
  await window.flush();

  const checkpoint = JSON.parse(await readFile(filePath, "utf8"));
  assert.equal(checkpoint.lastId, "c");
  assert.equal(window.lastCommittedId(), "c");
});

test("checkpoint window slides once the gap is filled", async () => {
  const directory = await tempDir();
  const filePath = join(directory, "checkpoint.json");
  const window = createCheckpointWindow({ filePath });

  await window.report({ seq: 1, lastId: "b", ok: true });
  await window.report({ seq: 0, lastId: "a", ok: true });
  await window.flush();

  const checkpoint = JSON.parse(await readFile(filePath, "utf8"));
  assert.equal(checkpoint.lastId, "b");
  assert.equal(window.lastCommittedId(), "b");
});

test("checkpoint writes once when several seqs commit together", async () => {
  let writes = 0;
  const window = createCheckpointWindow({
    filePath: "memory",
    write: async () => {
      writes += 1;
    },
  });

  await window.report({ seq: 1, lastId: "b", ok: true });
  await window.report({ seq: 2, lastId: "c", ok: true });
  await window.report({ seq: 3, lastId: "d", ok: true });
  await window.report({ seq: 4, lastId: "e", ok: true });
  assert.equal(writes, 0);
  await window.report({ seq: 0, lastId: "a", ok: true });
  await window.flush();
  assert.equal(writes, 1);
  assert.equal(window.lastCommittedId(), "e");
});

test("readFailedIds uniquely parses mixed id types", async () => {
  const directory = await tempDir();
  const filePath = join(directory, "failed-ids.jsonl");
  await writeFile(
    filePath,
    [
      JSON.stringify({ _id: "user_abc", reason: "x" }),
      JSON.stringify({ _id: "1001", _idType: "string", reason: "x" }),
      JSON.stringify({ _id: "user_abc", reason: "dup" }),
      JSON.stringify({ _id: "507f1f77bcf86cd799439011", reason: "x" }),
      "",
    ].join("\n"),
    "utf8",
  );

  const ids = await readFailedIds(filePath);
  assert.equal(ids[0], "user_abc");
  assert.equal(ids[1], "1001");
  assert.equal(ids[2].toHexString(), "507f1f77bcf86cd799439011");
  assert.equal(ids.length, 3);
});

test("appendFailedIds serializes concurrent writers and pruneFailedIds drops successes", async () => {
  const directory = await tempDir();
  const filePath = join(directory, "failed-ids.jsonl");
  await Promise.all([
    appendFailedIds(filePath, ["a", "b"], "x"),
    appendFailedIds(filePath, ["c"], "y"),
  ]);

  const before = await readFile(filePath, "utf8");
  assert.equal(before.trim().split("\n").length, 3);

  await pruneFailedIds(filePath, ["a", "c"]);
  const remaining = await readFailedIds(filePath);
  assert.deepEqual(remaining, ["b"]);
});

test("appendFailedIds rejects when the path cannot be written", async () => {
  const directory = await tempDir();
  await assert.rejects(
    () => appendFailedIds(join(directory, "missing", "failed.jsonl"), [1, 2], "x"),
    /ENOENT/,
  );
});

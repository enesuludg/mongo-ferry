import assert from "node:assert/strict";
import test from "node:test";
import { ObjectId } from "mongodb";
import {
  buildCursorFilter,
  ensureProjectionKeepsId,
  hasCursorSort,
  isAscendingIdSort,
  mergeAndFilters,
  objectIdFromTime,
  parseExtendedJson,
  parseSince,
  parseSort,
} from "../src/filter.js";
import { parseIdList } from "../src/ids.js";

test("parseExtendedJson hydrates $oid and $date", () => {
  const query = parseExtendedJson(
    '{"_id":{"$oid":"507f1f77bcf86cd799439011"},"createdAt":{"$gte":{"$date":"2026-08-15T00:00:00.000Z"}}}',
  );

  assert.equal(query._id.toHexString(), "507f1f77bcf86cd799439011");
  assert.equal(query.createdAt.$gte.toISOString(), "2026-08-15T00:00:00.000Z");
});

test("parseSince accepts duration and ISO date", () => {
  const fromDuration = parseSince("2d");
  const delta = Date.now() - fromDuration.getTime();
  assert.ok(delta > 1.5 * 24 * 60 * 60 * 1000);
  assert.ok(delta < 2.5 * 24 * 60 * 60 * 1000);
  assert.equal(parseSince("2026-08-15T00:00:00.000Z").toISOString(), "2026-08-15T00:00:00.000Z");
});

test("default window is ObjectId time unless query, ids, or dateField are set", () => {
  const since = new Date("2026-08-15T00:00:00.000Z");
  const defaultFilter = buildCursorFilter({
    query: {},
    ids: [],
    dateField: undefined,
    since,
    migrateAll: false,
    resumeAfter: null,
  });

  assert.deepEqual(defaultFilter, { _id: { $gte: objectIdFromTime(since) } });

  const withDateField = buildCursorFilter({
    query: {},
    ids: [],
    dateField: "createdAt",
    since,
    migrateAll: false,
    resumeAfter: null,
  });
  assert.deepEqual(withDateField, { createdAt: { $gte: since } });

  const explicit = buildCursorFilter({
    query: { status: "active" },
    ids: [],
    dateField: undefined,
    since,
    migrateAll: false,
    resumeAfter: null,
  });
  assert.deepEqual(explicit, { status: "active" });
});

test("ids and resumeAfter are AND-merged for any id type", () => {
  const ids = parseIdList("user_abc,1001");
  const resumeAfter = "user_aaa";
  const filter = buildCursorFilter({
    query: {},
    ids,
    dateField: undefined,
    since: new Date(),
    migrateAll: false,
    resumeAfter,
  });

  assert.deepEqual(filter, {
    $and: [{ _id: { $in: ids } }, { _id: { $gt: resumeAfter } }],
  });
});

test("mergeAndFilters skips empty clauses", () => {
  assert.deepEqual(mergeAndFilters({}, { a: 1 }, {}), { a: 1 });
});

test("projection always keeps _id", () => {
  assert.deepEqual(ensureProjectionKeepsId({ name: 1, _id: 0 }), { name: 1, _id: 1 });
  assert.equal(ensureProjectionKeepsId(undefined), undefined);
});

test("isAscendingIdSort only accepts {_id:1}", () => {
  assert.equal(isAscendingIdSort({ _id: 1 }), true);
  assert.equal(isAscendingIdSort({ createdAt: 1 }), false);
  assert.equal(isAscendingIdSort({ _id: 1, createdAt: 1 }), false);
  assert.equal(isAscendingIdSort(null), false);
  assert.equal(isAscendingIdSort({}), false);
});

test("parseSort defaults to _id unless dateField or none", () => {
  assert.deepEqual(parseSort(), { _id: 1 });
  assert.equal(parseSort(undefined, { dateField: "updatedAt" }), null);
  assert.equal(parseSort("none"), null);
  assert.equal(parseSort("{}"), null);
  assert.deepEqual(parseSort('{"updatedAt":1}'), { updatedAt: 1 });
  assert.equal(hasCursorSort(null), false);
  assert.equal(hasCursorSort({ updatedAt: 1 }), true);
});

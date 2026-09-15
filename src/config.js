import { readFile } from "node:fs/promises";
import { readCheckpoint, readFailedIds } from "./checkpoint.js";
import {
  buildCursorFilter,
  ensureProjectionKeepsId,
  hasExplicitQuery,
  isAscendingIdSort,
  parseExtendedJson,
  parseInteger,
  parsePositiveInteger,
  parseSince,
  parseSort,
} from "./filter.js";
import { assertIdType, idKey, parseId, parseIdList } from "./ids.js";
import { resolveDatabaseName } from "./mongo.js";

const DEFAULTS = {
  collection: "users",
  since: "30d",
  batchSize: 1000,
  concurrency: 16,
  sourcePoolSize: 16,
  targetPoolSize: 64,
  writeConcern: "1",
  checkpointFile: ".migrate-checkpoint.json",
  failedFile: "failed-ids.jsonl",
  onConflict: "replace",
  idType: "auto",
};

const ON_CONFLICT_MODES = new Set(["replace", "skip", "merge"]);

export async function loadConfig(args, env) {
  const collection = first(args.collection, env.COLLECTION, DEFAULTS.collection);
  const targetCollection = first(args.targetCollection, env.TARGET_COLLECTION, collection);
  const dateField = first(args.dateField, env.DATE_FIELD);
  const idType = assertIdType(first(args.idType, env.ID_TYPE, DEFAULTS.idType));
  const queryRaw = args.query ? args.query : args.queryFile ? await readFile(args.queryFile, "utf8") : "";
  const query = parseExtendedJson(queryRaw);
  const sort = parseSort(args.sort, { dateField });
  const projection = ensureProjectionKeepsId(args.projection ? parseExtendedJson(args.projection) : undefined);
  const retryFailedFile = args.retryFailed;
  const retryIds = retryFailedFile ? await readFailedIds(retryFailedFile, idType) : [];
  const ids = uniqueIds([...parseIdList(args.ids, idType), ...retryIds]);
  const sinceSpecified = Boolean(args.since);
  const migrateAll = Boolean(args.all) || (!sinceSpecified && !dateField);
  const resumeRequested = Boolean(args.resume);
  const checkpointFile = first(args.checkpointFile, env.CHECKPOINT_FILE, DEFAULTS.checkpointFile);
  const skip = args.skip === undefined ? undefined : parseInteger(args.skip, 0);

  if ((resumeRequested || args.resumeAfter) && !isAscendingIdSort(sort)) {
    throw new Error('--resume and --resumeAfter require the default sort {"_id":1}');
  }
  if ((resumeRequested || args.resumeAfter) && skip) {
    throw new Error("--skip cannot be combined with --resume/--resumeAfter");
  }

  const resumeAfter = args.resumeAfter
    ? parseId(args.resumeAfter, idType)
    : resumeRequested
      ? await readCheckpoint(checkpointFile, idType)
      : null;

  const sourceUri = required(first(args.source, env.SOURCE_URI), "SOURCE_URI / --source");
  const targetUri = required(first(args.target, env.TARGET_URI), "TARGET_URI / --target");
  const sourceDb = resolveDatabaseName(first(args.sourceDb, env.SOURCE_DB), sourceUri);
  const targetDb = resolveDatabaseName(first(args.targetDb, env.TARGET_DB), targetUri);

  if (!sourceDb) {
    throw new Error("Source database is required (--sourceDb, SOURCE_DB, or URI path)");
  }
  if (!targetDb) {
    throw new Error("Target database is required (--targetDb, TARGET_DB, or URI path)");
  }

  const onConflict = first(args.onConflict, env.ON_CONFLICT, DEFAULTS.onConflict);
  if (!ON_CONFLICT_MODES.has(onConflict)) {
    throw new Error(`Invalid --onConflict ${onConflict}. Use replace, skip, or merge`);
  }

  const since = parseSince(first(args.since, env.SINCE, DEFAULTS.since));
  const usedDefaultDateWindow = !migrateAll && ids.length === 0 && !hasExplicitQuery(query);
  const filter = buildCursorFilter({
    query,
    ids,
    dateField,
    since,
    migrateAll,
    resumeAfter,
  });

  return {
    sourceUri,
    targetUri,
    sourceDb,
    targetDb,
    collection,
    targetCollection,
    filter,
    sort,
    projection,
    hint: resolveHint({
      rawHint: args.hint,
      dateField,
      sort,
      ids,
      query,
    }),
    limit: args.limit === undefined ? undefined : parseInteger(args.limit, undefined),
    skip,
    batchSize: parsePositiveInteger(first(args.batchSize, env.BATCH_SIZE), DEFAULTS.batchSize),
    concurrency: parsePositiveInteger(first(args.concurrency, env.CONCURRENCY), DEFAULTS.concurrency),
    sourcePoolSize: parsePositiveInteger(first(args.sourcePoolSize, env.SOURCE_POOL_SIZE), DEFAULTS.sourcePoolSize),
    targetPoolSize: parsePositiveInteger(first(args.targetPoolSize, env.TARGET_POOL_SIZE), DEFAULTS.targetPoolSize),
    writeConcern: first(args.writeConcern, env.WRITE_CONCERN, DEFAULTS.writeConcern),
    onConflict,
    idType,
    dryRun: Boolean(args.dryRun),
    verify: Boolean(args.verify) || Boolean(args.verifyOnly),
    verifyOnly: Boolean(args.verifyOnly),
    stopOnError: Boolean(args.stopOnError),
    directSource: Boolean(args.directSource),
    directTarget: Boolean(args.directTarget),
    checkpointFile,
    failedFile: first(args.failedFile, env.FAILED_FILE, DEFAULTS.failedFile),
    retryFailedFile,
    retryIds,
    dateField,
    since,
    resumeAfter,
    migrateAll,
    usedDefaultDateWindow,
    usedObjectIdWindow: usedDefaultDateWindow && !dateField,
  };
}

function resolveHint({ rawHint, dateField, sort, ids, query }) {
  if (rawHint) {
    return parseExtendedJson(rawHint);
  }
  if (!dateField && ids.length === 0 && !hasExplicitQuery(query) && isAscendingIdSort(sort)) {
    return { _id: 1 };
  }
  return undefined;
}

function uniqueIds(ids) {
  const seen = new Set();
  const unique = [];
  for (const id of ids) {
    const key = idKey(id);
    if (seen.has(key)) {
      continue;
    }
    seen.add(key);
    unique.push(id);
  }
  return unique;
}

function first(...values) {
  return values.find((value) => value !== undefined && value !== null && value !== "");
}

function required(value, label) {
  if (!value) {
    throw new Error(`${label} is required`);
  }
  return value;
}

const BOOLEAN_FLAGS = new Set([
  "help",
  "h",
  "dryRun",
  "all",
  "stopOnError",
  "resume",
  "directSource",
  "directTarget",
  "verify",
  "verifyOnly",
]);

const VALUE_FLAGS = new Set([
  "source",
  "target",
  "sourceDb",
  "targetDb",
  "collection",
  "targetCollection",
  "query",
  "queryFile",
  "ids",
  "dateField",
  "since",
  "sort",
  "projection",
  "limit",
  "skip",
  "resumeAfter",
  "batchSize",
  "concurrency",
  "sourcePoolSize",
  "targetPoolSize",
  "writeConcern",
  "checkpointFile",
  "failedFile",
  "onConflict",
  "retryFailed",
  "idType",
]);

const KNOWN_FLAGS = new Set([...BOOLEAN_FLAGS, ...VALUE_FLAGS]);

export function parseArgs(argv) {
  const args = {};

  for (let index = 0; index < argv.length; index += 1) {
    const token = argv[index];
    if (!token.startsWith("--")) {
      throw new Error(`Unexpected argument: ${token}`);
    }

    const key = token.slice(2);
    if (!KNOWN_FLAGS.has(key)) {
      throw new Error(`Unknown flag --${key}`);
    }

    if (BOOLEAN_FLAGS.has(key)) {
      args[key] = true;
      continue;
    }

    const next = argv[index + 1];
    if (next === undefined || next.startsWith("--")) {
      throw new Error(`Flag --${key} requires a value`);
    }

    args[key] = next;
    index += 1;
  }

  return args;
}

export function printHelp() {
  const text = `
mongo-migrate — copy documents from a MongoDB 3.6 source to a 7.x target

Usage:
  node src/index.js --collection users
  node src/index.js --collection users --dateField createdAt --since 30d
  node src/index.js --collection users --query '{"status":"active"}'
  node src/index.js --collection users --ids user_abc,1001,507f1f77bcf86cd799439011
  node src/index.js --retryFailed failed-ids.jsonl --collection users

Connection:
  --source <uri>              Source URI (or SOURCE_URI)
  --target <uri>              Target URI (or TARGET_URI)
  --sourceDb <name>           Source database (or SOURCE_DB / URI path)
  --targetDb <name>           Target database (or TARGET_DB / URI path)
  --directSource              Direct connection to the source host (useful on 3.6)
  --directTarget              Direct connection to the target host

Cursor (same idea as a find cursor):
  --collection <name>         Source collection (default: users)
  --targetCollection <name>   Target collection (default: same as --collection)
  --query <ejson>             Extended JSON filter
  --queryFile <path>          Filter from a JSON file
  --ids <id,id>               Migrate only these _id values
  --idType <auto|objectId|string|number>  How to parse --ids / --resumeAfter (default: auto)
  --dateField <field>         Use a Date field for --since instead of ObjectId time
  --since <30d|ISO>           Lower bound (default: 30d)
  --all                       Ignore the default 30-day window
  --sort <ejson>              Cursor sort (default: {"_id":1}; resume requires this)
  --projection <ejson>        Optional projection (_id is always kept)
  --limit <n>                 Cursor limit
  --skip <n>                  Cursor skip
  --resumeAfter <id>          Continue after this _id (requires sort {_id:1})
  --resume                    Continue from checkpoint file

Throughput:
  --batchSize <n>             Docs per bulkWrite (default: 1000)
  --concurrency <n>           Parallel in-flight bulkWrites (default: 16)
  --sourcePoolSize <n>        Source maxPoolSize (default: 16)
  --targetPoolSize <n>        Target maxPoolSize (default: 64)
  --writeConcern <n|majority> Target write concern (default: 1)

Behavior:
  --onConflict <mode>         replace (default) | skip | merge
  --dryRun                    countDocuments + sample; do not write
  --verify                    After copy, fail only if any source _id is missing on the target
  --verifyOnly                Verify only; do not write
  --retryFailed <file>        Re-read failed-ids.jsonl, copy those _ids, then drop successes from the file
  --stopOnError               Abort on the first failed document
  --checkpointFile <path>     Contiguous successful prefix (default: .migrate-checkpoint.json)
  --failedFile <path>         Failed _ids (default: failed-ids.jsonl)
  --help                      Show this help

Default 30-day window is {_id: {$gte: ObjectId.createFromTime(now-30d)}} so MongoDB 3.6
can range-scan the _id index. Pass --dateField createdAt if you must filter on a Date field.
`.trim();

  console.log(text);
}

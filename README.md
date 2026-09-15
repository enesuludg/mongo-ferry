# mongo-ferry

[https://github.com/enesuludg/mongo-ferry](https://github.com/enesuludg/mongo-ferry)

Copy documents from a **MongoDB 3.6** source to a **MongoDB 7.x** target.

There is no supported in-place upgrade from 3.6 to 7. mongo-ferry streams a `find` cursor, batches documents, and writes them with **parallel in-flight** `bulkWrite`s. Existing target `_id`s are replaced by default (`--onConflict replace`).

The Node driver is pinned to **4.17.2** so one process can talk to both 3.6 and 7.x. MongoDB 3.6 is EOL and outside the current driver compatibility matrix; the pin is still the practical choice for a dual-version copy.

Requires **Node.js 18+**.

## Setup

```bash
git clone https://github.com/enesuludg/mongo-ferry.git
cd mongo-ferry
cp .env.example .env
npm install
```

Set `SOURCE_URI`, `TARGET_URI`, `SOURCE_DB`, and `TARGET_DB` in `.env`. Do not commit `.env`.

```bash
npm test
```

## Usage

Omit `--since` to copy the **entire collection**:

```bash
npm run migrate -- --collection users
```

Limit by ObjectId time (range scan on `_id`, no in-memory sort on 3.6):

```bash
npm run migrate -- --collection users --since 30d
```

```js
db.users.find({
  _id: { $gte: ObjectId.createFromTime(Math.floor(Date.now() / 1000) - 30 * 24 * 3600) }
}).sort({ _id: 1 }).hint({ _id: 1 })
```

ObjectId timestamps are second-resolution and only work for ObjectId `_id`s. To filter on a Date field instead:

```bash
npm run migrate -- --collection users --dateField updatedAt --since 15d
```

`--dateField` does **not** sort by `_id` unless you pass `--sort '{"_id":1}'`. Sorting a date filter by `_id` on 3.6 can force an in-memory sort. `--resume` still requires `{_id:1}`. mongo-ferry does not guess a `--dateField` index hint (3.6 returns `bad hint` if the key pattern does not exist). Pass `--hint` only with a key pattern from `db.collection.getIndexes()`. Throughput is usually limited by fetching full documents from the source, not `--concurrency`; `--projection` and running near the source help more. `--onConflict skip` inserts only missing `_id`s and will not refresh already-copied docs.

```bash
npm run migrate -- \
  --collection users \
  --targetCollection users_v7 \
  --query '{"status":"active"}' \
  --batchSize 1000 \
  --concurrency 16 \
  --onConflict replace
```

`--concurrency` is the number of **in-flight bulkWrites**. The cursor only waits for a free slot, then keeps reading.

`--ids` parsing is controlled by `--idType` (`auto` by default). `auto` treats 24-char hex as ObjectId and digit-only values as numbers. If `_id` is actually a string like `"1001"` or a 24-char hex string, pass `--idType string`.

```bash
npm run migrate -- --collection users --ids user_abc,1001 --idType string
```

Unknown flags and flags missing a value are errors. `--skip` cannot be combined with `--resume` / `--resumeAfter`.

```bash
node src/index.js --help
```

## Conflicts

| `--onConflict` | Target already has `_id` |
| --- | --- |
| `replace` (default) | Overwrite the whole document |
| `skip` | Leave the target document unchanged |
| `merge` | `$set` source fields; extra target fields stay |

## Throughput

| Flag | Default | Role |
| --- | --- | --- |
| `--batchSize` | `1000` | Documents per `bulkWrite` |
| `--concurrency` | `16` | Parallel in-flight bulk writes |
| `--sourcePoolSize` | `16` | Source connection pool |
| `--targetPoolSize` | `64` | Target connection pool |
| `--writeConcern` | `1` | Fastest durable-enough write; use `majority` if you need it |

Writes use `ordered: false`. Partial `writeErrors` fail only the errored indexes. Permanent errors (auth, validation, duplicate key, `TypeError`) are not retried. Sockets time out after 5 minutes.

Progress and the final line report **created** (upsert insert) vs **updated** (already existed) from the `bulkWrite` result — no extra queries. The run also prints a wall-clock `took …` timer after clients close.

## Resume and failed ids

Checkpointing stores the last `_id` of the contiguous batch prefix after both **successful** seqs and **failed seqs whose `_id`s were actually written to `failed-ids.jsonl`**. If that file cannot be written, the window **does not move** and the process exits 1, so `--resume` cannot skip ids that have no recovery path.

Recover persisted failures with `--retryFailed`. Only ids that were **attempted and succeeded** are removed from that file; unattempted ids stay if the run is interrupted or aborted.

`--resume` / `--resumeAfter` require `sort: {_id:1}` (the default when `--dateField` is not set).

```bash
npm run migrate -- --collection users --resume
npm run migrate -- --retryFailed failed-ids.jsonl --collection users
```

`SIGINT` / `SIGTERM` drain in-flight writes, then exit **1** with `migration interrupted`. Any run with `failed > 0` also exits **1** so cron/CI sees a partial copy. `--stopOnError` drains, flushes the checkpoint, and closes clients before exiting.

## Dry run and verify

```bash
npm run migrate -- --collection users --dryRun
npm run migrate -- --collection users --verify
npm run migrate -- --collection users --verifyOnly
```

`--dryRun` uses `countDocuments` plus a 5-document sample (honors `--skip` / `--limit`). `--verify` fails only when a **source** `_id` is missing on the target. Extra documents already on the target are expected and do not fail the run.

## Notes

- Target indexes are not copied. Create them on 7.x separately.
- `--projection` always keeps `_id`.
- Use a dedicated user with `find` on the source and `insert` / `update` on the target.
- `.env`, `.migrate-checkpoint.json`, and `failed-ids.jsonl` are gitignored.

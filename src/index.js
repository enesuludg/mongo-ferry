import "dotenv/config";
import { parseArgs, printHelp } from "./cli.js";
import { loadConfig } from "./config.js";
import { logger } from "./logger.js";
import { runMigration } from "./migrator.js";
import { createSourceClient, createTargetClient, pingClient } from "./mongo.js";
import { describeRunOutcome, resolveExitCode } from "./outcome.js";
import { serializeForLog } from "./serialize.js";
import { verifyCopy } from "./verify.js";

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || args.h) {
    printHelp();
    return;
  }

  const config = await loadConfig(args, process.env);
  logPlan(config);

  if (config.dateField && config.usedDefaultDateWindow) {
    logger.warn(
      "filtering on a Date field while sorting by _id can force an in-memory sort on MongoDB 3.6; prefer the default ObjectId time window",
    );
  }

  const sourceClient = createSourceClient(config.sourceUri, config.sourcePoolSize, config.directSource);
  const targetClient = createTargetClient(
    config.targetUri,
    config.targetPoolSize,
    config.writeConcern,
    config.directTarget,
  );

  let stopRequested = false;
  const onSignal = (signal) => {
    if (stopRequested) {
      logger.warn(`received ${signal} again, forcing exit`);
      process.exit(1);
    }
    stopRequested = true;
    logger.warn(`received ${signal}, draining in-flight writes`);
  };

  process.on("SIGINT", () => onSignal("SIGINT"));
  process.on("SIGTERM", () => onSignal("SIGTERM"));

  try {
    await sourceClient.connect();
    await targetClient.connect();
    await pingClient(sourceClient, "source");
    await pingClient(targetClient, "target");

    const sourceCollection = sourceClient.db(config.sourceDb).collection(config.collection);
    const targetCollection = targetClient.db(config.targetDb).collection(config.targetCollection);

    if (!config.verifyOnly) {
      const result = await runMigration({
        sourceCollection,
        targetCollection,
        config,
        shouldStop: () => stopRequested,
      });
      const exitCode = resolveExitCode(result);
      const message = describeRunOutcome(result);
      if (exitCode === 0) {
        logger.info(message, result);
      } else {
        logger.warn(message, result);
        process.exitCode = exitCode;
      }
    }

    if (config.verify && !stopRequested) {
      const report = await verifyCopy({ sourceCollection, targetCollection, config });
      if (report.missing > 0) {
        throw new Error(`verify failed: missing=${report.missing} source=${report.sourceCount} target=${report.targetCount}`);
      }
    }
  } finally {
    await Promise.allSettled([sourceClient.close(), targetClient.close()]);
  }
}

function logPlan(config) {
  logger.info("starting migration", {
    collection: config.collection,
    targetCollection: config.targetCollection,
    sourceDb: config.sourceDb,
    targetDb: config.targetDb,
    batchSize: config.batchSize,
    concurrency: config.concurrency,
    sourcePoolSize: config.sourcePoolSize,
    targetPoolSize: config.targetPoolSize,
    writeConcern: config.writeConcern,
    onConflict: config.onConflict,
    dryRun: config.dryRun,
    verify: config.verify,
    verifyOnly: config.verifyOnly,
    resumeAfter: config.resumeAfter === null || config.resumeAfter === undefined ? null : String(config.resumeAfter),
    dateWindow: config.usedDefaultDateWindow
      ? {
        field: config.dateField ?? "_id",
        since: config.since.toISOString(),
        objectIdTime: Boolean(config.usedObjectIdWindow),
      }
      : null,
    filter: serializeForLog(config.filter),
  });
}

main().catch((error) => {
  logger.error(error.message);
  process.exitCode = 1;
});

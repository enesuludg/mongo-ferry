export function resolveExitCode(result) {
  if (!result || result.interrupted || result.failed > 0) {
    return 1;
  }
  return 0;
}

export function formatDuration(ms) {
  const elapsed = Math.max(0, Math.round(Number(ms) || 0));
  if (elapsed < 1000) {
    return `${elapsed}ms`;
  }

  const totalSeconds = Math.floor(elapsed / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${minutes}m ${seconds}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${seconds}s`;
  }
  return `${seconds}s`;
}

export function describeRunOutcome(result) {
  const duration = Number.isFinite(result?.elapsedMs) ? ` in ${formatDuration(result.elapsedMs)}` : "";
  const counts = formatWriteCounts(result);
  if (result?.interrupted) {
    return `migration interrupted${duration}${counts}`;
  }
  if (result?.failed > 0) {
    return `migration finished with failures${duration}${counts}`;
  }
  return `migration finished${duration}${counts}`;
}

export function formatWriteCounts(result) {
  if (!result || (result.created === undefined && result.updated === undefined)) {
    return "";
  }
  return `: created ${result.created ?? 0}, updated ${result.updated ?? 0}`;
}


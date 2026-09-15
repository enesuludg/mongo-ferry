export function resolveExitCode(result) {
  if (!result || result.interrupted || result.failed > 0) {
    return 1;
  }
  return 0;
}

export function describeRunOutcome(result) {
  if (result?.interrupted) {
    return "migration interrupted";
  }
  if (result?.failed > 0) {
    return "migration finished with failures";
  }
  return "migration finished";
}

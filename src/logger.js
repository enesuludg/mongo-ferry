const formatTime = () => new Date().toISOString();

const write = (level, message, extra) => {
  const line = extra
    ? `${formatTime()} ${level} ${message} ${JSON.stringify(extra)}`
    : `${formatTime()} ${level} ${message}`;
  const stream = level === "ERROR" ? process.stderr : process.stdout;
  stream.write(`${line}\n`);
};

export const logger = {
  info: (message, extra) => write("INFO", message, extra),
  warn: (message, extra) => write("WARN", message, extra),
  error: (message, extra) => write("ERROR", message, extra),
};

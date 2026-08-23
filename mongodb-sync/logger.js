const winston = require('winston');
const path = require('path');
const fs = require('fs');

// Log directory is configurable so it can be mounted as a Docker volume.
// Defaults to ./logs inside the container (mapped to a named volume in compose).
const logDir = process.env.LOG_DIR || path.join(__dirname, 'logs');

if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

const logFormat = winston.format.combine(
  winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
  winston.format.errors({ stack: true }),
  winston.format.printf(({ timestamp, level, message, ...meta }) => {
    return `[${timestamp}] [${level.toUpperCase()}] ${message} ${
      Object.keys(meta).length ? JSON.stringify(meta, null, 2) : ''
    }`;
  })
);

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: logFormat,
  transports: [
    // Console (captured by `docker logs` / journald)
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        logFormat
      )
    }),
    // All logs
    new winston.transports.File({
      filename: path.join(logDir, 'combined.log'),
      maxsize: 5242880, // 5MB
      maxFiles: 5
    }),
    // Errors only
    new winston.transports.File({
      filename: path.join(logDir, 'errors.log'),
      level: 'error',
      maxsize: 5242880,
      maxFiles: 5
    }),
    // Oplog / change-stream operations
    new winston.transports.File({
      filename: path.join(logDir, 'operations.log'),
      maxsize: 10485760, // 10MB
      maxFiles: 10
    })
  ]
});

module.exports = logger;

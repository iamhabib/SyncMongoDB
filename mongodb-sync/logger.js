const winston = require('winston');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');

const logDir = process.env.LOG_DIR || path.join(__dirname, 'logs');

function ensureDir(dir) {
  try {
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    return true;
  } catch (err) {
    // Prefer console-only over crashing the sync process (common with bind mounts)
    console.error(`[logger] Cannot create log directory ${dir}: ${err.message}`);
    return false;
  }
}

ensureDir(logDir);

// Custom transport for daily rotating files with automatic Gzip compression and retention cleanup
class DailyFileTransport extends winston.Transport {
  constructor(options) {
    super(options);
    this.dirname = options.dirname || logDir;
    this.subDir = options.subDir || '';
    this.level = options.level;
    this.retentionDays = options.retentionDays || 60;
    this.currentDate = this.getFormattedDate();
    this.disabled = false;
    this.stream = this.createStream();
    if (this.stream) {
      this.cleanupOldLogs();
    }
  }

  getFormattedDate() {
    const d = new Date();
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  createStream() {
    if (this.disabled) return null;

    const dateStr = this.getFormattedDate();
    const filename = `${dateStr}.log`;
    const dir = path.join(this.dirname, this.subDir);
    if (!ensureDir(dir)) {
      this.disabled = true;
      return null;
    }

    try {
      const filepath = path.join(dir, filename);
      return fs.createWriteStream(filepath, { flags: 'a' });
    } catch (err) {
      console.error(`[logger] Cannot open log file in ${dir}: ${err.message}`);
      this.disabled = true;
      return null;
    }
  }

  compressLogFile(date) {
    const filename = `${date}.log`;
    const dir = path.join(this.dirname, this.subDir);
    const filepath = path.join(dir, filename);
    const gzipPath = `${filepath}.gz`;

    if (!fs.existsSync(filepath)) return;

    const gzip = zlib.createGzip();
    const source = fs.createReadStream(filepath);
    const destination = fs.createWriteStream(gzipPath);

    source.pipe(gzip).pipe(destination);

    destination.on('finish', () => {
      fs.unlink(filepath, () => {});
    });

    source.on('error', () => {});
    destination.on('error', () => {});
  }

  cleanupOldLogs() {
    try {
      const dir = path.join(this.dirname, this.subDir);
      if (!fs.existsSync(dir)) return;

      const files = fs.readdirSync(dir);
      const retentionMs = this.retentionDays * 24 * 60 * 60 * 1000;
      const cutoffTime = Date.now() - retentionMs;

      for (const file of files) {
        if (!file.endsWith('.log') && !file.endsWith('.log.gz')) continue;
        const filepath = path.join(dir, file);
        const stats = fs.statSync(filepath);
        if (stats.mtimeMs < cutoffTime) {
          fs.unlinkSync(filepath);
        }
      }
    } catch (err) {
      // Fail silently — logging must not crash the process
    }
  }

  log(info, callback) {
    setImmediate(() => this.emit('logged', info));

    if (this.disabled || !this.stream) {
      callback();
      return;
    }

    const dateStr = this.getFormattedDate();
    if (dateStr !== this.currentDate) {
      const oldDate = this.currentDate;
      this.currentDate = dateStr;
      this.stream.end();
      this.compressLogFile(oldDate);
      this.stream = this.createStream();
      if (this.stream) {
        this.cleanupOldLogs();
      }
    }

    if (!this.stream) {
      callback();
      return;
    }

    const output = `${info[Symbol.for('message')] || info.message}\n`;
    this.stream.write(output);
    callback();
  }
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

const transports = [
  new winston.transports.Console({
    format: winston.format.combine(winston.format.colorize(), logFormat)
  })
];

const combinedTransport = new DailyFileTransport({
  subDir: 'combined',
  retentionDays: 60
});
if (!combinedTransport.disabled) {
  transports.push(combinedTransport);
}

const errorTransport = new DailyFileTransport({
  subDir: 'errors',
  level: 'error',
  retentionDays: 60
});
if (!errorTransport.disabled) {
  transports.push(errorTransport);
}

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: logFormat,
  transports
});

module.exports = logger;

const winston = require('winston');
const path = require('path');
const fs = require('fs');
const zlib = require('zlib');

const logDir = process.env.LOG_DIR || path.join(__dirname, 'logs');

if (!fs.existsSync(logDir)) {
  fs.mkdirSync(logDir, { recursive: true });
}

// Custom transport for daily rotating files with automatic Gzip compression and retention cleanup
class DailyFileTransport extends winston.Transport {
  constructor(options) {
    super(options);
    this.dirname = options.dirname || logDir;
    this.subDir = options.subDir || '';
    this.level = options.level;
    this.retentionDays = options.retentionDays || 60;
    this.currentDate = this.getFormattedDate();
    this.stream = this.createStream();
    
    // Run cleanup on initialization
    this.cleanupOldLogs();
  }

  getFormattedDate() {
    const d = new Date();
    const year = d.getFullYear();
    const month = String(d.getMonth() + 1).padStart(2, '0');
    const day = String(d.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  createStream() {
    const dateStr = this.getFormattedDate();
    const filename = `${dateStr}.log`;
    const dir = path.join(this.dirname, this.subDir);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
    const filepath = path.join(dir, filename);
    return fs.createWriteStream(filepath, { flags: 'a' });
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
      fs.unlink(filepath, (err) => {
        // Handle error silently
      });
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
      // Fail silently
    }
  }

  log(info, callback) {
    setImmediate(() => this.emit('logged', info));

    const dateStr = this.getFormattedDate();
    if (dateStr !== this.currentDate) {
      const oldDate = this.currentDate;
      this.currentDate = dateStr;
      this.stream.end();
      
      // Compress the completed day's file
      this.compressLogFile(oldDate);

      this.stream = this.createStream();
      this.cleanupOldLogs();
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

const logger = winston.createLogger({
  level: process.env.LOG_LEVEL || 'info',
  format: logFormat,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        logFormat
      )
    }),
    new DailyFileTransport({
      subDir: 'combined',
      retentionDays: 60
    }),
    new DailyFileTransport({
      subDir: 'errors',
      level: 'error',
      retentionDays: 60
    }),
    new DailyFileTransport({
      subDir: 'operations',
      retentionDays: 60
    })
  ]
});

module.exports = logger;

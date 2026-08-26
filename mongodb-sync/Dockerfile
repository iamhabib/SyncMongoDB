FROM node:20-alpine

WORKDIR /app

RUN apk add --no-cache su-exec

# Install dependencies first for better layer caching
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev

# Copy application source
COPY . .

# Non-root user for security
RUN addgroup -S appgroup && adduser -S appuser -G appgroup \
    && mkdir -p /app/logs/combined /app/logs/errors \
    && chown -R appuser:appgroup /app \
    && chmod +x /app/docker-entrypoint.sh

ENV NODE_ENV=production \
    LOG_DIR=/app/logs

EXPOSE 3000

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "require('http').get('http://127.0.0.1:'+(process.env.PORT||3000)+'/health',r=>process.exit(r.statusCode===200?0:1)).on('error',()=>process.exit(1))"

# Start as root so entrypoint can chown the bind-mounted log volume, then drop privileges
ENTRYPOINT ["/app/docker-entrypoint.sh"]
CMD ["node", "app.js"]

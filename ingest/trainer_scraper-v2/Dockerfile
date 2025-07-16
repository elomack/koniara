# ingest/trainer_scraper-v2/Dockerfile
FROM node:20-slim

# Create app directory
WORKDIR /app

# Install only production deps
COPY package.json package-lock.json* ./
RUN npm install --production

# Copy source
COPY . .

# No HTTP server; this is a job that runs index.js
ENTRYPOINT ["node", "index.js"]

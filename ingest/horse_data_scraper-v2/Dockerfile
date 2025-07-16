# ingest/horse_data_scraper-v2/Dockerfile
FROM node:20-slim

# Create app directory
WORKDIR /app

# Install only production dependencies
COPY package.json package-lock.json* ./
RUN npm install --production

# Copy the rest of the source code
COPY . .

# This job runs index.js directly; no HTTP server needed
ENTRYPOINT ["node", "index.js"]

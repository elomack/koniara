# ingest/<scraper>-v2/Dockerfile
FROM node:20-slim

WORKDIR /app

# Install production deps only
COPY package.json package-lock.json* ./
RUN npm install --production

# Copy scraper code
COPY . .

# Run the scraper as a batch job
ENTRYPOINT ["node", "index.js"]

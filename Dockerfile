FROM node:22-bookworm-slim

ENV NODE_ENV=production
ENV YOUTUBE_COOKIES_FILE=/app/secrets/youtube-cookies.txt
WORKDIR /app

RUN apt-get update \
  && apt-get install -y --no-install-recommends ffmpeg python3 python3-pip ca-certificates \
  && python3 -m pip install --break-system-packages --no-cache-dir yt-dlp \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts \
  && npm cache clean --force
COPY src ./src
COPY scripts ./scripts
RUN mkdir -p /app/secrets \
  && chown -R node:node /app

USER node
EXPOSE 8080
CMD ["sh", "-c", "touch \"$YOUTUBE_COOKIES_FILE\" && chmod 600 \"$YOUTUBE_COOKIES_FILE\" && exec node src/server.mjs"]

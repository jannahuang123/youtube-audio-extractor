import { spawn } from 'node:child_process';
import { randomUUID, timingSafeEqual } from 'node:crypto';
import { createReadStream, promises as fs } from 'node:fs';
import { mkdtemp, readdir, rm, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import {
  DeleteObjectCommand,
  GetObjectCommand,
  PutObjectCommand,
  S3Client,
} from '@aws-sdk/client-s3';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { z } from 'zod';

const MAX_AUDIO_BYTES = 100 * 1024 * 1024;
const MAX_REQUEST_BYTES = 64 * 1024;
const MAX_DURATION_SECONDS = 2 * 60 * 60;
const SIGNED_URL_SECONDS = 15 * 60;
const OBJECT_RETENTION_MS = 30 * 60 * 1000;
const MAX_CONCURRENT = Math.max(
  1,
  Number.parseInt(process.env.MAX_CONCURRENT ?? '2', 10) || 2
);

const config = {
  port: Number.parseInt(process.env.PORT ?? '8080', 10) || 8080,
  token: process.env.INTERNAL_API_TOKEN ?? '',
  cookiesFile:
    process.env.YOUTUBE_COOKIES_FILE ?? '/run/secrets/youtube-cookies.txt',
  r2Endpoint: process.env.R2_ENDPOINT ?? '',
  r2Bucket: process.env.R2_BUCKET ?? '',
  r2AccessKey: process.env.R2_ACCESS_KEY ?? '',
  r2SecretKey: process.env.R2_SECRET_KEY ?? '',
  r2Region: process.env.R2_REGION ?? 'auto',
  forcePathStyle: process.env.R2_FORCE_PATH_STYLE === 'true',
};

if (!config.token) throw new Error('INTERNAL_API_TOKEN is required');
if (
  !config.r2Endpoint ||
  !config.r2Bucket ||
  !config.r2AccessKey ||
  !config.r2SecretKey
) {
  throw new Error(
    'R2_ENDPOINT, R2_BUCKET, R2_ACCESS_KEY and R2_SECRET_KEY are required'
  );
}

const requestSchema = z
  .object({
    url: z.string().trim().min(1).max(2048),
    language: z.enum(['auto', 'en', 'zh', 'es', 'fr', 'de']).default('auto'),
    maxDurationSeconds: z
      .number()
      .int()
      .min(1)
      .max(MAX_DURATION_SECONDS)
      .default(MAX_DURATION_SECONDS),
  })
  .strict();

const videoIdPattern = /^[A-Za-z0-9_-]{11}$/;
const allowedHosts = new Set([
  'youtube.com',
  'www.youtube.com',
  'm.youtube.com',
  'youtu.be',
]);

function parseYoutubeUrl(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    return null;
  }
  const host = url.hostname.toLowerCase();
  if (
    url.protocol !== 'https:' ||
    !allowedHosts.has(host) ||
    url.username ||
    url.password ||
    (url.port && url.port !== '443')
  )
    return null;
  if (url.searchParams.has('list') || url.searchParams.has('playlist'))
    return null;

  let videoId = null;
  if (host === 'youtu.be') {
    const parts = url.pathname.split('/').filter(Boolean);
    if (parts.length === 1) videoId = parts[0];
  } else if (url.pathname === '/watch') {
    videoId = url.searchParams.get('v');
  } else {
    const parts = url.pathname.split('/').filter(Boolean);
    if (parts.length === 2 && parts[0].toLowerCase() === 'shorts')
      videoId = parts[1];
  }
  return videoId && videoIdPattern.test(videoId) ? { videoId } : null;
}

function json(res, status, payload) {
  const body = JSON.stringify(payload);
  res.writeHead(status, {
    'content-type': 'application/json; charset=utf-8',
    'cache-control': 'no-store',
    'content-length': Buffer.byteLength(body),
  });
  res.end(body);
}

function authorized(req) {
  const actual = req.headers.authorization ?? '';
  const expected = `Bearer ${config.token}`;
  const a = Buffer.from(actual);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

async function readJson(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > MAX_REQUEST_BYTES) throw new HttpError(413, 'request_too_large');
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new HttpError(400, 'invalid_json');
  }
}

class HttpError extends Error {
  constructor(status, code) {
    super(code);
    this.status = status;
    this.code = code;
  }
}

function runCommand(
  command,
  args,
  { timeoutMs, maxOutputBytes = 256 * 1024 } = {}
) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    const stdout = [];
    const stderr = [];
    let outputBytes = 0;
    let settled = false;
    const timer = setTimeout(() => {
      child.kill('SIGKILL');
      reject(new HttpError(502, 'extractor_timeout'));
    }, timeoutMs);
    timer.unref?.();

    const collect = (target) => (chunk) => {
      outputBytes += chunk.length;
      if (outputBytes <= maxOutputBytes) target.push(chunk);
    };
    child.stdout.on('data', collect(stdout));
    child.stderr.on('data', collect(stderr));
    child.on('error', (error) => {
      clearTimeout(timer);
      if (!settled) {
        settled = true;
        reject(
          error.code === 'ENOENT'
            ? new HttpError(503, 'extractor_unavailable')
            : new HttpError(502, 'extractor_failed')
        );
      }
    });
    child.on('close', (code) => {
      clearTimeout(timer);
      if (settled) return;
      settled = true;
      if (code !== 0) {
        const error = new HttpError(502, 'extractor_failed');
        error.stderr = Buffer.concat(stderr).toString('utf8').slice(-2000);
        reject(error);
        return;
      }
      resolve({
        stdout: Buffer.concat(stdout).toString('utf8'),
        stderr: Buffer.concat(stderr).toString('utf8'),
      });
    });
  });
}

async function validateCookiesFile() {
  const info = await stat(config.cookiesFile);
  if (!info.isFile() || (info.mode & 0o077) !== 0)
    throw new Error('YOUTUBE_COOKIES_FILE must be a private regular file');
}

const s3 = new S3Client({
  region: config.r2Region,
  endpoint: config.r2Endpoint,
  forcePathStyle: config.forcePathStyle,
  credentials: {
    accessKeyId: config.r2AccessKey,
    secretAccessKey: config.r2SecretKey,
  },
});

async function extractAudio({ videoId, url, maxDurationSeconds }) {
  const workdir = await mkdtemp(join(tmpdir(), 'youtube-audio-'));
  const metadataPath = join(workdir, 'metadata.json');
  const sourceTemplate = join(workdir, 'source.%(ext)s');
  const outputPath = join(workdir, 'audio.m4a');
  try {
    const metadata = await runCommand(
      'yt-dlp',
      [
        '--cookies',
        config.cookiesFile,
        '--no-playlist',
        '--no-warnings',
        '--dump-single-json',
        '--skip-download',
        '--',
        url,
      ],
      { timeoutMs: 120_000, maxOutputBytes: 2 * 1024 * 1024 }
    );
    await fs.writeFile(metadataPath, metadata.stdout, { mode: 0o600 });
    let info;
    try {
      info = JSON.parse(metadata.stdout);
    } catch {
      throw new HttpError(502, 'invalid_video_metadata');
    }
    const duration = Number(info.duration);
    if (!Number.isFinite(duration) || duration <= 0)
      throw new HttpError(422, 'video_duration_unavailable');
    if (duration > maxDurationSeconds)
      throw new HttpError(413, 'video_too_long');

    await runCommand(
      'yt-dlp',
      [
        '--cookies',
        config.cookiesFile,
        '--no-playlist',
        '--no-warnings',
        '--newline',
        '--restrict-filenames',
        '--max-filesize',
        '100M',
        '-f',
        'ba[ext=m4a]/ba',
        '-o',
        sourceTemplate,
        '--',
        url,
      ],
      { timeoutMs: 15 * 60 * 1000, maxOutputBytes: 256 * 1024 }
    );

    const files = (await readdir(workdir)).filter((name) =>
      name.startsWith('source.')
    );
    if (files.length !== 1) throw new HttpError(502, 'audio_download_failed');
    const sourcePath = join(workdir, files[0]);
    await runCommand(
      'ffmpeg',
      [
        '-hide_banner',
        '-loglevel',
        'error',
        '-y',
        '-i',
        sourcePath,
        '-vn',
        '-c:a',
        'aac',
        '-b:a',
        '128k',
        '-movflags',
        '+faststart',
        outputPath,
      ],
      { timeoutMs: 15 * 60 * 1000, maxOutputBytes: 64 * 1024 }
    );
    const outputInfo = await stat(outputPath);
    if (outputInfo.size <= 0)
      throw new HttpError(502, 'audio_conversion_failed');
    if (outputInfo.size > MAX_AUDIO_BYTES)
      throw new HttpError(413, 'audio_too_large');

    return {
      title: String(info.title || videoId).slice(0, 500),
      outputPath,
      size: outputInfo.size,
      workdir,
    };
  } catch (error) {
    await rm(workdir, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}

async function uploadAndSign({ videoId, title, outputPath, size }) {
  const key = `youtube-audio/${videoId}/${randomUUID()}.m4a`;
  await s3.send(
    new PutObjectCommand({
      Bucket: config.r2Bucket,
      Key: key,
      Body: createReadStream(outputPath),
      ContentLength: size,
      ContentType: 'audio/mp4',
      CacheControl: 'private, max-age=900',
      Metadata: {
        source: 'youtube-transcript',
        videoid: videoId,
        title: encodeURIComponent(title).slice(0, 512),
      },
    })
  );
  const expiresAt = new Date(Date.now() + SIGNED_URL_SECONDS * 1000);
  const downloadUrl = await getSignedUrl(
    s3,
    new GetObjectCommand({ Bucket: config.r2Bucket, Key: key }),
    { expiresIn: SIGNED_URL_SECONDS }
  );
  const cleanup = setTimeout(() => {
    s3.send(
      new DeleteObjectCommand({ Bucket: config.r2Bucket, Key: key })
    ).catch((error) =>
      console.error('object cleanup failed', error?.name || 'unknown')
    );
  }, OBJECT_RETENTION_MS);
  cleanup.unref?.();
  return {
    title,
    mimeType: 'audio/mp4',
    downloadUrl,
    expiresAt: expiresAt.toISOString(),
  };
}

const cached = new Map();
const inFlight = new Map();
let active = 0;

async function extractOnce(input) {
  const existing = cached.get(input.videoId);
  if (existing && Date.parse(existing.expiresAt) > Date.now() + 30_000)
    return existing;
  cached.delete(input.videoId);
  if (inFlight.has(input.videoId)) return inFlight.get(input.videoId);
  if (active >= MAX_CONCURRENT) throw new HttpError(429, 'extractor_busy');

  active += 1;
  const task = (async () => {
    let result;
    let workdir;
    try {
      const audio = await extractAudio(input);
      workdir = audio.workdir;
      result = await uploadAndSign({ videoId: input.videoId, ...audio });
      cached.set(input.videoId, result);
      return result;
    } finally {
      active -= 1;
      inFlight.delete(input.videoId);
      if (workdir)
        await rm(workdir, { recursive: true, force: true }).catch(() => {});
    }
  })();
  inFlight.set(input.videoId, task);
  return task;
}

async function handleAudio(req, res) {
  if (!authorized(req)) return json(res, 401, { error: 'unauthorized' });
  const body = requestSchema.safeParse(await readJson(req));
  if (!body.success) return json(res, 400, { error: 'invalid_request' });
  const youtube = parseYoutubeUrl(body.data.url);
  if (!youtube) return json(res, 400, { error: 'invalid_youtube_url' });
  try {
    const result = await extractOnce({
      ...youtube,
      url: body.data.url,
      maxDurationSeconds: body.data.maxDurationSeconds,
    });
    return json(res, 200, result);
  } catch (error) {
    const status = error instanceof HttpError ? error.status : 502;
    const code = error instanceof HttpError ? error.code : 'extractor_failed';
    console.error('youtube extraction failed', {
      code,
      videoId: youtube.videoId,
    });
    return json(res, status, { error: code });
  }
}

const server = createServer(async (req, res) => {
  try {
    if (req.method === 'GET' && req.url === '/healthz')
      return json(res, 200, { ok: true, active });
    if (req.method === 'POST' && req.url === '/v1/youtube/audio')
      return await handleAudio(req, res);
    return json(res, 404, { error: 'not_found' });
  } catch (error) {
    const status = error instanceof HttpError ? error.status : 500;
    const code = error instanceof HttpError ? error.code : 'internal_error';
    console.error('request failed', { code });
    return json(res, status, { error: code });
  }
});

await validateCookiesFile();
server.listen(config.port, '0.0.0.0', () =>
  console.log(`youtube extractor listening on ${config.port}`)
);

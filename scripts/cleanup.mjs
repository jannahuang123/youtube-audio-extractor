import {
  DeleteObjectsCommand,
  ListObjectsV2Command,
  S3Client,
} from '@aws-sdk/client-s3';

const required = ['R2_ENDPOINT', 'R2_BUCKET', 'R2_ACCESS_KEY', 'R2_SECRET_KEY'];
for (const name of required) {
  if (!process.env[name]) throw new Error(`${name} is required`);
}

const s3 = new S3Client({
  region: process.env.R2_REGION ?? 'auto',
  endpoint: process.env.R2_ENDPOINT,
  forcePathStyle: process.env.R2_FORCE_PATH_STYLE === 'true',
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY,
    secretAccessKey: process.env.R2_SECRET_KEY,
  },
});

const cutoff = Date.now() - 30 * 60 * 1000;
let continuationToken;
let deleted = 0;
do {
  const page = await s3.send(
    new ListObjectsV2Command({
      Bucket: process.env.R2_BUCKET,
      Prefix: 'youtube-audio/',
      ContinuationToken: continuationToken,
    })
  );
  const stale = (page.Contents ?? [])
    .filter(
      (item) =>
        item.Key && item.LastModified && item.LastModified.getTime() < cutoff
    )
    .map((item) => ({ Key: item.Key }));
  if (stale.length) {
    await s3.send(
      new DeleteObjectsCommand({
        Bucket: process.env.R2_BUCKET,
        Delete: { Objects: stale, Quiet: true },
      })
    );
    deleted += stale.length;
  }
  continuationToken = page.IsTruncated ? page.NextContinuationToken : undefined;
} while (continuationToken);
console.log(`deleted ${deleted} stale youtube audio objects`);

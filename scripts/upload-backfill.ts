import "dotenv/config";

import { readFile } from "node:fs/promises";
import path from "node:path";

import { HeadObjectCommand, PutObjectCommand } from "@aws-sdk/client-s3";

import { s3Client } from "../src/s3";
import { OUTPUT_ROOT, getS3KeyFromLocalPath, listFilesRecursively } from "./backfill-local";

const DEFAULT_CONCURRENCY = Number(process.env.BACKFILL_UPLOAD_CONCURRENCY ?? "8");

function getBucketName(): string {
  const bucketName = process.env.S3_BUCKET ?? process.env.OBJECT_STORAGE_BUCKET ?? "";
  if (!bucketName) {
    throw new Error("Missing S3 bucket configuration. Set S3_BUCKET or OBJECT_STORAGE_BUCKET.");
  }

  return bucketName;
}

async function collectUploadFiles(): Promise<string[]> {
  const directories = [path.join(OUTPUT_ROOT, "raw"), path.join(OUTPUT_ROOT, "rollups")];
  const files = await Promise.all(
    directories.map(async (directory) => {
      try {
        return await listFilesRecursively(directory);
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") {
          return [];
        }

        throw error;
      }
    }),
  );

  return files.flat().filter((filePath) => filePath.endsWith(".json") || filePath.endsWith(".ndjson")).sort();
}

async function uploadFile(bucketName: string, filePath: string): Promise<string> {
  const key = getS3KeyFromLocalPath(OUTPUT_ROOT, filePath);
  const body = await readFile(filePath);

  await s3Client.send(
    new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: body,
      ContentType: key.endsWith(".ndjson") ? "application/x-ndjson" : "application/json",
      CacheControl: key.startsWith("raw/") ? "no-cache" : undefined,
    }),
  );

  return key;
}

async function runWithConcurrency<T>(items: T[], concurrency: number, worker: (item: T, index: number) => Promise<void>): Promise<void> {
  let nextIndex = 0;

  const runners = Array.from({ length: Math.min(concurrency, items.length) }, async () => {
    while (true) {
      const currentIndex = nextIndex;
      nextIndex += 1;

      if (currentIndex >= items.length) {
        return;
      }

      await worker(items[currentIndex], currentIndex);
    }
  });

  await Promise.all(runners);
}

async function verifyKeys(bucketName: string, keys: string[]): Promise<void> {
  for (const key of keys) {
    await s3Client.send(
      new HeadObjectCommand({
        Bucket: bucketName,
        Key: key,
      }),
    );
  }
}

async function main(): Promise<void> {
  const bucketName = getBucketName();
  const files = await collectUploadFiles();

  if (files.length === 0) {
    console.log(`No backfill files found under ${OUTPUT_ROOT}.`);
    return;
  }

  console.log(`Uploading ${files.length} backfill file(s) to s3://${bucketName}...`);

  const uploadedKeys: string[] = [];

  await runWithConcurrency(files, DEFAULT_CONCURRENCY, async (filePath, index) => {
    const key = await uploadFile(bucketName, filePath);
    uploadedKeys[index] = key;

    if ((index + 1) % 500 === 0 || index === files.length - 1) {
      console.log(`Uploaded ${index + 1}/${files.length}: ${key}`);
    }
  });

  const rawSample = uploadedKeys.find((key) => key.startsWith("raw/"));
  const hourlySample = uploadedKeys.find((key) => key.includes("/hourly/"));
  const dailySample = uploadedKeys.find((key) => key.includes("/daily/"));
  const verificationKeys = [rawSample, hourlySample, dailySample].filter((key): key is string => Boolean(key));

  await verifyKeys(bucketName, verificationKeys);

  console.log(`Upload complete. Verified ${verificationKeys.length} sample object(s) in s3://${bucketName}.`);
}

void main().catch((error) => {
  console.error("Backfill upload failed.", error);
  process.exitCode = 1;
});

import { mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";

export const OUTPUT_ROOT = path.resolve(process.cwd(), "backfill-data");

function encodeLocalFilename(segment: string): string {
  return segment.replaceAll(":", "__");
}

function decodeLocalFilename(segment: string): string {
  return segment.replaceAll("__", ":");
}

export function getLocalPathForS3Key(root: string, key: string): string {
  const segments = key.split("/");
  const localSegments = segments.map((segment, index) => {
    if (index !== segments.length - 1) {
      return segment;
    }

    return encodeLocalFilename(segment);
  });

  return path.join(root, ...localSegments);
}

export function getS3KeyFromLocalPath(root: string, filePath: string): string {
  const relativePath = path.relative(root, filePath);
  const segments = relativePath.split(path.sep);

  return segments
    .map((segment, index) => {
      if (index !== segments.length - 1) {
        return segment;
      }

      return decodeLocalFilename(segment);
    })
    .join("/");
}

export async function writeJsonLocal(root: string, key: string, data: unknown): Promise<void> {
  const filePath = getLocalPathForS3Key(root, key);
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

export async function writeNdjsonLocal(root: string, key: string, rows: unknown[]): Promise<void> {
  const filePath = getLocalPathForS3Key(root, key);
  await mkdir(path.dirname(filePath), { recursive: true });
  await writeFile(filePath, rows.map((row) => JSON.stringify(row)).join("\n"), "utf8");
}

export async function readJsonLocal<T>(root: string, key: string): Promise<T | null> {
  const filePath = getLocalPathForS3Key(root, key);

  try {
    const text = await readFile(filePath, "utf8");
    return JSON.parse(text) as T;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return null;
    }

    throw error;
  }
}

export async function readNdjsonLocal<T>(root: string, key: string): Promise<T[]> {
  const filePath = getLocalPathForS3Key(root, key);

  try {
    const text = await readFile(filePath, "utf8");
    return text
      .split("\n")
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) => JSON.parse(line) as T);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return [];
    }

    throw error;
  }
}

export async function listFilesRecursively(targetDirectory: string): Promise<string[]> {
  const entries = await readdir(targetDirectory, { withFileTypes: true });
  const nested = await Promise.all(
    entries.map(async (entry) => {
      const fullPath = path.join(targetDirectory, entry.name);
      if (entry.isDirectory()) {
        return listFilesRecursively(fullPath);
      }

      return [fullPath];
    }),
  );

  return nested.flat().sort();
}



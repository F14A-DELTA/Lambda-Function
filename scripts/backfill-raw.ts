import "dotenv/config";

import { mkdir } from "node:fs/promises";
import path from "node:path";

import OpenElectricityClient from "openelectricity";

import { OUTPUT_ROOT, writeJsonLocal } from "./backfill-local";
import { buildSnapshotFromRows, getRows, type DataRow } from "../src/normalise";
import { getRawKey } from "../src/s3";
import type { NetworkCode } from "../src/types";

const DEFAULT_START_DATE = "2026-03-12";
const REQUEST_DELAY_MS = Number(process.env.BACKFILL_REQUEST_DELAY_MS ?? "400");
const EMPTY_STREAK_STOP_DAYS = Number(process.env.BACKFILL_EMPTY_STREAK_STOP_DAYS ?? "7");

type CliOptions = {
  startDate: string;
  endDate?: string;
  maxDays?: number;
  networks: NetworkCode[];
};

function toIsoWithoutMillis(date: Date): string {
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

function formatDayUtc(date: Date): string {
  return [
    date.getUTCFullYear(),
    String(date.getUTCMonth() + 1).padStart(2, "0"),
    String(date.getUTCDate()).padStart(2, "0"),
  ].join("-");
}

function parseDay(day: string): Date {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!match) {
    throw new Error(`Invalid day "${day}". Expected YYYY-MM-DD.`);
  }

  const [, year, month, date] = match;
  return new Date(Date.UTC(Number(year), Number(month) - 1, Number(date)));
}

function shiftDay(day: string, delta: number): string {
  const parsed = parseDay(day);
  parsed.setUTCDate(parsed.getUTCDate() + delta);
  return formatDayUtc(parsed);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseArgs(argv: string[]): CliOptions {
  const args = new Map<string, string>();

  for (const arg of argv) {
    if (!arg.startsWith("--")) continue;
    const eqIndex = arg.indexOf("=");
    if (eqIndex === -1) {
      args.set(arg.slice(2), "true");
      continue;
    }

    args.set(arg.slice(2, eqIndex), arg.slice(eqIndex + 1));
  }

  const startDate = args.get("start-date") ?? process.env.BACKFILL_START_DATE ?? DEFAULT_START_DATE;
  const endDate = args.get("end-date") ?? process.env.BACKFILL_END_DATE;
  const maxDaysValue = args.get("max-days") ?? process.env.BACKFILL_MAX_DAYS;
  const networksValue = args.get("networks") ?? process.env.BACKFILL_NETWORKS ?? "NEM,WEM";

  parseDay(startDate);
  if (endDate) parseDay(endDate);

  const maxDays = maxDaysValue ? Number(maxDaysValue) : undefined;
  if (maxDays !== undefined && (!Number.isFinite(maxDays) || maxDays <= 0)) {
    throw new Error(`Invalid max days "${maxDaysValue}". Expected a positive integer.`);
  }

  const networks = networksValue
    .split(",")
    .map((value) => value.trim().toUpperCase())
    .filter(Boolean) as NetworkCode[];

  if (networks.length === 0 || networks.some((value) => value !== "NEM" && value !== "WEM")) {
    throw new Error(`Invalid networks "${networksValue}". Expected NEM and/or WEM.`);
  }

  return { startDate, endDate, maxDays, networks };
}

function getRowsByTimestamp(rows: DataRow[]): Map<string, DataRow[]> {
  const rowsByTimestamp = new Map<string, DataRow[]>();

  for (const row of rows) {
    const timestamp = toIsoWithoutMillis(row.interval);
    const existing = rowsByTimestamp.get(timestamp) ?? [];
    existing.push(row);
    rowsByTimestamp.set(timestamp, existing);
  }

  return rowsByTimestamp;
}

async function fetchDayForNetwork(
  client: OpenElectricityClient,
  network: NetworkCode,
  day: string,
): Promise<{ snapshotsWritten: number; timestamps: string[] }> {
  const dateStart = `${day}T00:00:00`;
  const dateEnd = `${shiftDay(day, 1)}T00:00:00`;

  const generationData = await client.getNetworkData(network, ["power", "energy", "market_value"], {
    interval: "5m",
    dateStart,
    dateEnd,
    primaryGrouping: "network_region",
    secondaryGrouping: ["fueltech"],
  });

  const marketData = await client.getMarket(network, ["price", "demand", "curtailment_solar_utility", "curtailment_wind"], {
    interval: "5m",
    dateStart,
    dateEnd,
    primaryGrouping: "network_region",
  });

  const emissionsData = await client.getNetworkData(network, ["emissions"], {
    interval: "5m",
    dateStart,
    dateEnd,
    primaryGrouping: "network_region",
  });

  const generationRows = getRows(generationData);
  const marketRows = getRows(marketData);
  const emissionsRows = getRows(emissionsData);

  const generationByTimestamp = getRowsByTimestamp(generationRows);
  const marketByTimestamp = getRowsByTimestamp(marketRows);
  const emissionsByTimestamp = getRowsByTimestamp(emissionsRows);

  const timestamps = Array.from(
    new Set([...generationByTimestamp.keys(), ...marketByTimestamp.keys(), ...emissionsByTimestamp.keys()]),
  ).sort((left, right) => new Date(left).getTime() - new Date(right).getTime());

  for (const timestamp of timestamps) {
    const snapshot = buildSnapshotFromRows(
      generationByTimestamp.get(timestamp) ?? [],
      marketByTimestamp.get(timestamp) ?? [],
      emissionsByTimestamp.get(timestamp) ?? [],
      network,
    );

    await writeJsonLocal(OUTPUT_ROOT, getRawKey(network, new Date(timestamp)), snapshot);
  }

  return { snapshotsWritten: timestamps.length, timestamps };
}

async function ensureOutputFolders(): Promise<void> {
  await mkdir(path.join(OUTPUT_ROOT, "raw"), { recursive: true });
}

async function main(): Promise<void> {
  const options = parseArgs(process.argv.slice(2));
  const client = new OpenElectricityClient({
    apiKey: process.env.OPENELECTRICITY_API_KEY,
    baseUrl: process.env.OPENELECTRICITY_BASE_URL,
  });

  await ensureOutputFolders();

  console.log(
    `Starting raw backfill into ${OUTPUT_ROOT} from ${options.startDate} backwards` +
      (options.endDate ? ` until ${options.endDate}` : "") +
      (options.maxDays ? ` for up to ${options.maxDays} day(s)` : "") +
      ` across ${options.networks.join(", ")}.`,
  );

  let day = options.startDate;
  let processedDays = 0;
  let emptyDayStreak = 0;
  let totalSnapshotsWritten = 0;

  while (true) {
    if (options.endDate && parseDay(day).getTime() < parseDay(options.endDate).getTime()) {
      break;
    }

    if (options.maxDays && processedDays >= options.maxDays) {
      break;
    }

    let wroteAnythingForDay = false;

    for (const network of options.networks) {
      console.log(`[${day}] Fetching ${network}...`);

      const result = await fetchDayForNetwork(client, network, day);
      totalSnapshotsWritten += result.snapshotsWritten;
      wroteAnythingForDay = wroteAnythingForDay || result.snapshotsWritten > 0;

      console.log(
        `[${day}] ${network}: wrote ${result.snapshotsWritten} raw snapshot file(s)` +
          (result.timestamps.length > 0
            ? ` from ${result.timestamps[0]} to ${result.timestamps[result.timestamps.length - 1]}`
            : "."),
      );

      await sleep(REQUEST_DELAY_MS);
    }

    processedDays += 1;

    if (wroteAnythingForDay) {
      emptyDayStreak = 0;
    } else {
      emptyDayStreak += 1;
      console.log(`[${day}] No data found for any requested network. Empty streak: ${emptyDayStreak}.`);
    }

    if (!options.endDate && emptyDayStreak >= EMPTY_STREAK_STOP_DAYS) {
      console.log(`Stopping after ${EMPTY_STREAK_STOP_DAYS} consecutive empty day(s).`);
      break;
    }

    day = shiftDay(day, -1);
  }

  console.log(
    `Backfill complete. Processed ${processedDays} day(s) and wrote ${totalSnapshotsWritten} raw snapshot file(s).`,
  );
}

void main().catch((error) => {
  console.error("Raw backfill failed.", error);
  process.exitCode = 1;
});

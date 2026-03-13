import "dotenv/config";

import path from "node:path";

import { getDailyRollupKey, getHourlyRollupKey } from "../src/s3";
import type { EnergySnapshot, NetworkCode, RegionSnapshot, RollupRow } from "../src/types";
import { OUTPUT_ROOT, getS3KeyFromLocalPath, listFilesRecursively, readJsonLocal, readNdjsonLocal, writeNdjsonLocal } from "./backfill-local";

function round(value: number | null | undefined, decimals: number): number | null {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return null;
  }

  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function average(values: number[]): number | null {
  if (values.length === 0) {
    return null;
  }

  return values.reduce((total, value) => total + value, 0) / values.length;
}

type RegionGenAccumulator = Map<string, Array<{
  power_mw: number | null;
  price_dollar_per_mwh: number | null;
  proportion_pct: number | null;
  total_energy_mwh: number | null;
}>>;

type RegionMarketAccumulator = Map<string, Array<{
  price: number | null;
  demand: number | null;
  renewables_pct: number | null;
  net_generation_mw: number | null;
  renewables_mw: number | null;
}>>;

type RegionEmissionsAccumulator = Map<string, Array<{ volume: number | null; intensity: number | null }>>;

function aggregateSnapshotsToRollupRows(snapshots: EnergySnapshot[], bucket: string, network: NetworkCode): RollupRow[] {
  const genGroups: RegionGenAccumulator = new Map();
  const marketGroups: RegionMarketAccumulator = new Map();
  const emissionsGroups: RegionEmissionsAccumulator = new Map();
  const networkEmissions: Array<{ volume: number | null; intensity: number | null }> = [];

  for (const snapshot of snapshots) {
    for (const [region, regionData] of Object.entries(snapshot.regions) as Array<[string, RegionSnapshot | undefined]>) {
      if (!regionData) continue;

      for (const item of regionData.generation) {
        const key = `${region}|${item.fueltech}`;
        const rows = genGroups.get(key) ?? [];
        rows.push({
          power_mw: item.power_mw,
          price_dollar_per_mwh: item.price_dollar_per_mwh,
          proportion_pct: item.proportion_pct,
          total_energy_mwh: item.total_energy_mwh ?? null,
        });
        genGroups.set(key, rows);
      }

      for (const item of regionData.loads) {
        const key = `${region}|${item.fueltech}`;
        const rows = genGroups.get(key) ?? [];
        rows.push({
          power_mw: item.power_mw,
          price_dollar_per_mwh: item.price_dollar_per_mwh,
          proportion_pct: item.proportion_pct,
          total_energy_mwh: item.total_energy_mwh ?? null,
        });
        genGroups.set(key, rows);
      }

      const marketRows = marketGroups.get(region) ?? [];
      marketRows.push({
        price: regionData.price_dollar_per_mwh,
        demand: regionData.demand_mw,
        renewables_pct: regionData.summary.renewables_pct,
        net_generation_mw: regionData.summary.net_generation_mw,
        renewables_mw: regionData.summary.renewables_mw,
      });
      marketGroups.set(region, marketRows);

      const emissionsRows = emissionsGroups.get(region) ?? [];
      emissionsRows.push({
        volume: regionData.emissions.volume_tco2e_per_30m,
        intensity: regionData.emissions.intensity_kgco2e_per_mwh,
      });
      emissionsGroups.set(region, emissionsRows);
    }

    networkEmissions.push({
      volume: snapshot.emissions.volume_tco2e_per_30m,
      intensity: snapshot.emissions.intensity_kgco2e_per_mwh,
    });
  }

  const generationRows: RollupRow[] = Array.from(genGroups.entries()).map(([key, rows]) => {
    const pipeIndex = key.indexOf("|");
    const region = key.slice(0, pipeIndex);
    const fueltech = key.slice(pipeIndex + 1);

    return {
      bucket,
      network,
      region,
      fueltech,
      avg_power_mw: round(average(rows.map((row) => row.power_mw).filter((value): value is number => typeof value === "number")), 1),
      avg_price_per_mwh: round(
        average(rows.map((row) => row.price_dollar_per_mwh).filter((value): value is number => typeof value === "number")),
        2,
      ),
      avg_proportion_pct: round(
        average(rows.map((row) => row.proportion_pct).filter((value): value is number => typeof value === "number")),
        1,
      ),
      total_energy_mwh: round(rows.reduce((total, row) => total + Number(row.total_energy_mwh ?? 0), 0), 1),
    };
  });

  const marketRows: RollupRow[] = Array.from(marketGroups.entries()).map(([region, rows]) => ({
    bucket,
    network,
    region,
    avg_price_dollar_per_mwh: round(average(rows.map((row) => row.price).filter((value): value is number => typeof value === "number")), 2),
    avg_demand_mw: round(average(rows.map((row) => row.demand).filter((value): value is number => typeof value === "number")), 1),
    avg_renewables_pct: round(
      average(rows.map((row) => row.renewables_pct).filter((value): value is number => typeof value === "number")),
      1,
    ),
    avg_net_generation_mw: round(
      average(rows.map((row) => row.net_generation_mw).filter((value): value is number => typeof value === "number")),
      1,
    ),
    avg_renewables_mw: round(
      average(rows.map((row) => row.renewables_mw).filter((value): value is number => typeof value === "number")),
      1,
    ),
  }));

  const emissionsRows: RollupRow[] = Array.from(emissionsGroups.entries()).map(([region, rows]) => ({
    bucket,
    network,
    region,
    total_emissions_tco2e: round(rows.reduce((total, row) => total + Number(row.volume ?? 0), 0), 1),
    avg_intensity_kgco2e_per_mwh: round(
      average(rows.map((row) => row.intensity).filter((value): value is number => typeof value === "number")),
      2,
    ),
  }));

  const networkSummaryRow: RollupRow = {
    bucket,
    network,
    total_emissions_tco2e: round(networkEmissions.reduce((total, row) => total + Number(row.volume ?? 0), 0), 1),
    avg_intensity_kgco2e_per_mwh: round(
      average(networkEmissions.map((row) => row.intensity).filter((value): value is number => typeof value === "number")),
      2,
    ),
  };

  return [...generationRows, ...marketRows, ...emissionsRows, networkSummaryRow];
}

function aggregateRollupRows(rows: RollupRow[], bucket: string, network: NetworkCode): RollupRow[] {
  const groups = new Map<string, RollupRow[]>();

  for (const row of rows) {
    const key = `${row.region ?? ""}|${row.fueltech ?? ""}`;
    const existing = groups.get(key) ?? [];
    existing.push(row);
    groups.set(key, existing);
  }

  return Array.from(groups.entries()).map(([key, groupRows]) => {
    const pipeIndex = key.indexOf("|");
    const region = key.slice(0, pipeIndex);
    const fueltech = key.slice(pipeIndex + 1);

    return {
      bucket,
      network,
      region: region || undefined,
      fueltech: fueltech || undefined,
      avg_power_mw: round(
        average(groupRows.map((row) => row.avg_power_mw).filter((value): value is number => typeof value === "number")),
        1,
      ),
      avg_price_per_mwh: round(
        average(groupRows.map((row) => row.avg_price_per_mwh).filter((value): value is number => typeof value === "number")),
        2,
      ),
      avg_proportion_pct: round(
        average(groupRows.map((row) => row.avg_proportion_pct).filter((value): value is number => typeof value === "number")),
        1,
      ),
      total_energy_mwh: round(groupRows.reduce((total, row) => total + Number(row.total_energy_mwh ?? 0), 0), 1),
      avg_price_dollar_per_mwh: round(
        average(
          groupRows.map((row) => row.avg_price_dollar_per_mwh).filter((value): value is number => typeof value === "number"),
        ),
        2,
      ),
      avg_demand_mw: round(
        average(groupRows.map((row) => row.avg_demand_mw).filter((value): value is number => typeof value === "number")),
        1,
      ),
      avg_renewables_pct: round(
        average(groupRows.map((row) => row.avg_renewables_pct).filter((value): value is number => typeof value === "number")),
        1,
      ),
      avg_net_generation_mw: round(
        average(groupRows.map((row) => row.avg_net_generation_mw).filter((value): value is number => typeof value === "number")),
        1,
      ),
      avg_renewables_mw: round(
        average(groupRows.map((row) => row.avg_renewables_mw).filter((value): value is number => typeof value === "number")),
        1,
      ),
      total_emissions_tco2e: round(groupRows.reduce((total, row) => total + Number(row.total_emissions_tco2e ?? 0), 0), 1),
      avg_intensity_kgco2e_per_mwh: round(
        average(
          groupRows.map((row) => row.avg_intensity_kgco2e_per_mwh).filter((value): value is number => typeof value === "number"),
        ),
        2,
      ),
    };
  });
}

function getHourBucket(timestamp: string): string {
  return `${timestamp.slice(0, 13)}:00:00Z`;
}

function getDayBucketFromHour(hourBucket: string): string {
  return hourBucket.slice(0, 10);
}

async function getRawTimestampsForNetwork(network: NetworkCode): Promise<string[]> {
  const targetDirectory = path.join(OUTPUT_ROOT, "raw", `network=${network}`);

  try {
    const files = await listFilesRecursively(targetDirectory);
    return files
      .filter((filePath) => filePath.endsWith(".json"))
      .map((filePath) => getS3KeyFromLocalPath(OUTPUT_ROOT, filePath))
      .map((key) => key.slice(key.lastIndexOf("/") + 1, -".json".length))
      .sort((left, right) => new Date(left).getTime() - new Date(right).getTime());
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return [];
    }

    throw error;
  }
}

async function buildHourlyRollups(network: NetworkCode): Promise<string[]> {
  const timestamps = await getRawTimestampsForNetwork(network);
  const hourBuckets = Array.from(new Set(timestamps.map((timestamp) => getHourBucket(timestamp))));
  const builtBuckets: string[] = [];

  for (const hourBucket of hourBuckets) {
    const hourStart = new Date(hourBucket);
    const rawKeys = Array.from({ length: 12 }, (_, index) => {
      const timestamp = new Date(hourStart);
      timestamp.setUTCMinutes(index * 5);
      return `raw/network=${network}/year=${timestamp.getUTCFullYear()}/month=${String(timestamp.getUTCMonth() + 1).padStart(2, "0")}/day=${String(timestamp.getUTCDate()).padStart(2, "0")}/${timestamp.toISOString().replace(/\.\d{3}Z$/, "Z")}.json`;
    });

    const snapshots = (
      await Promise.all(rawKeys.map((key) => readJsonLocal<EnergySnapshot>(OUTPUT_ROOT, key)))
    ).filter((snapshot): snapshot is EnergySnapshot => snapshot !== null);

    if (snapshots.length === 0) {
      continue;
    }

    const rollupKey = getHourlyRollupKey(
      network,
      hourStart.getUTCFullYear(),
      hourStart.getUTCMonth() + 1,
      hourStart.getUTCDate(),
      hourStart.getUTCHours(),
    );

    await writeNdjsonLocal(OUTPUT_ROOT, rollupKey, aggregateSnapshotsToRollupRows(snapshots, hourBucket, network));
    builtBuckets.push(hourBucket);
  }

  return builtBuckets;
}

async function buildDailyRollups(network: NetworkCode, hourBuckets: string[]): Promise<string[]> {
  const days = Array.from(new Set(hourBuckets.map((hourBucket) => getDayBucketFromHour(hourBucket))));
  const builtDays: string[] = [];

  for (const day of days) {
    const dayDate = new Date(`${day}T00:00:00Z`);
    const hourlyKeys = Array.from({ length: 24 }, (_, hour) =>
      getHourlyRollupKey(network, dayDate.getUTCFullYear(), dayDate.getUTCMonth() + 1, dayDate.getUTCDate(), hour),
    );

    const rows = (await Promise.all(hourlyKeys.map((key) => readNdjsonLocal<RollupRow>(OUTPUT_ROOT, key)))).flat();
    if (rows.length === 0) {
      continue;
    }

    const dailyKey = getDailyRollupKey(network, dayDate.getUTCFullYear(), dayDate.getUTCMonth() + 1, dayDate.getUTCDate());
    await writeNdjsonLocal(OUTPUT_ROOT, dailyKey, aggregateRollupRows(rows, day, network));
    builtDays.push(day);
  }

  return builtDays;
}

async function main(): Promise<void> {
  const networks: NetworkCode[] = ["NEM", "WEM"];

  for (const network of networks) {
    console.log(`Building local hourly rollups for ${network}...`);
    const hourBuckets = await buildHourlyRollups(network);
    console.log(`Built ${hourBuckets.length} hourly rollup file(s) for ${network}.`);

    console.log(`Building local daily rollups for ${network}...`);
    const dayBuckets = await buildDailyRollups(network, hourBuckets);
    console.log(`Built ${dayBuckets.length} daily rollup file(s) for ${network}.`);
  }

  console.log(`Local rollup build complete under ${OUTPUT_ROOT}.`);
}

void main().catch((error) => {
  console.error("Local rollup build failed.", error);
  process.exitCode = 1;
});

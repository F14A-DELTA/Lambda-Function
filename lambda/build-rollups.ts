import "dotenv/config";
import { getDailyRollupKey, getHourlyRollupKey, getJsonMany, getNdjsonMany, putNdjson } from "../src/s3";
import type { EnergySnapshot, NetworkCode, RegionSnapshot, RollupRow } from "../src/types";

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

function getPreviousHour(): Date {
  const previousHour = new Date();
  previousHour.setUTCMinutes(0, 0, 0);
  previousHour.setUTCHours(previousHour.getUTCHours() - 1);
  return previousHour;
}

function buildHourlyRawKeys(network: NetworkCode, previousHour: Date): string[] {
  return Array.from({ length: 12 }, (_, index) => {
    const timestamp = new Date(previousHour);
    timestamp.setUTCMinutes(index * 5);

    const year = timestamp.getUTCFullYear();
    const month = String(timestamp.getUTCMonth() + 1).padStart(2, "0");
    const day = String(timestamp.getUTCDate()).padStart(2, "0");
    const iso = timestamp.toISOString().replace(/\.\d{3}Z$/, "Z");

    return `raw/network=${network}/year=${year}/month=${month}/day=${day}/${iso}.json`;
  });
}

type RegionGenAccumulator = Map<string, Array<{ power_mw: number | null; price_dollar_per_mwh: number | null; proportion_pct: number | null; total_energy_mwh: number | null }>>;
type RegionMarketAccumulator = Map<string, Array<{ price: number | null; demand: number | null; renewables_pct: number | null; net_generation_mw: number | null; renewables_mw: number | null }>>;
type RegionEmissionsAccumulator = Map<string, Array<{ volume: number | null; intensity: number | null }>>;

function aggregateSnapshotsToRollupRows(snapshots: EnergySnapshot[], bucket: string, network: NetworkCode): RollupRow[] {
  // Keyed by "region|fueltech"
  const genGroups: RegionGenAccumulator = new Map();
  // Keyed by "region"
  const marketGroups: RegionMarketAccumulator = new Map();
  // Keyed by "region"
  const emissionsGroups: RegionEmissionsAccumulator = new Map();

  // For network-level backward compat
  const networkEmissions: Array<{ volume: number | null; intensity: number | null }> = [];

  for (const snapshot of snapshots) {
    // Per-region data from the richer region snapshots.
    for (const [region, regionData] of Object.entries(snapshot.regions) as Array<[string, RegionSnapshot | undefined]>) {
      if (!regionData) continue;

      // Generation rows by region+fueltech.
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

      // Load rows by region+fueltech.
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

      // Market summary row per region.
      const mRows = marketGroups.get(region) ?? [];
      mRows.push({
        price: regionData.price_dollar_per_mwh,
        demand: regionData.demand_mw,
        renewables_pct: regionData.summary.renewables_pct,
        net_generation_mw: regionData.summary.net_generation_mw,
        renewables_mw: regionData.summary.renewables_mw,
      });
      marketGroups.set(region, mRows);

      // Emissions per region.
      const eRows = emissionsGroups.get(region) ?? [];
      eRows.push({
        volume: regionData.emissions.volume_tco2e_per_30m,
        intensity: regionData.emissions.intensity_kgco2e_per_mwh,
      });
      emissionsGroups.set(region, eRows);
    }

    // Network-level emissions for backward compat (no region tag).
    networkEmissions.push({
      volume: snapshot.emissions.volume_tco2e_per_30m,
      intensity: snapshot.emissions.intensity_kgco2e_per_mwh,
    });
  }

  // Build region+fueltech generation rows.
  const generationRows: RollupRow[] = Array.from(genGroups.entries()).map(([key, rows]) => {
    const pipeIndex = key.indexOf("|");
    const region = key.slice(0, pipeIndex);
    const fueltech = key.slice(pipeIndex + 1);

    return {
      bucket,
      network,
      region,
      fueltech,
      avg_power_mw: round(
        average(rows.map((r) => r.power_mw).filter((v): v is number => typeof v === "number")),
        1,
      ),
      avg_price_per_mwh: round(
        average(rows.map((r) => r.price_dollar_per_mwh).filter((v): v is number => typeof v === "number")),
        2,
      ),
      avg_proportion_pct: round(
        average(rows.map((r) => r.proportion_pct).filter((v): v is number => typeof v === "number")),
        1,
      ),
      total_energy_mwh: round(
        rows.reduce((total, r) => total + Number(r.total_energy_mwh ?? 0), 0),
        1,
      ),
    };
  });

  // Build per-region market summary rows (no fueltech, used for price/demand/renewables/net gen stats).
  const marketRows: RollupRow[] = Array.from(marketGroups.entries()).map(([region, rows]) => ({
    bucket,
    network,
    region,
    avg_price_dollar_per_mwh: round(
      average(rows.map((r) => r.price).filter((v): v is number => typeof v === "number")),
      2,
    ),
    avg_demand_mw: round(
      average(rows.map((r) => r.demand).filter((v): v is number => typeof v === "number")),
      1,
    ),
    avg_renewables_pct: round(
      average(rows.map((r) => r.renewables_pct).filter((v): v is number => typeof v === "number")),
      1,
    ),
    avg_net_generation_mw: round(
      average(rows.map((r) => r.net_generation_mw).filter((v): v is number => typeof v === "number")),
      1,
    ),
    avg_renewables_mw: round(
      average(rows.map((r) => r.renewables_mw).filter((v): v is number => typeof v === "number")),
      1,
    ),
  }));

  // Build per-region emissions rows (no fueltech).
  const emissionsRows: RollupRow[] = Array.from(emissionsGroups.entries()).map(([region, rows]) => ({
    bucket,
    network,
    region,
    total_emissions_tco2e: round(
      rows.reduce((total, r) => total + Number(r.volume ?? 0), 0),
      1,
    ),
    avg_intensity_kgco2e_per_mwh: round(
      average(rows.map((r) => r.intensity).filter((v): v is number => typeof v === "number")),
      2,
    ),
  }));

  // Network-level summary row (no region, no fueltech) for backward compatibility.
  const networkSummaryRow: RollupRow = {
    bucket,
    network,
    total_emissions_tco2e: round(
      networkEmissions.reduce((total, r) => total + Number(r.volume ?? 0), 0),
      1,
    ),
    avg_intensity_kgco2e_per_mwh: round(
      average(networkEmissions.map((r) => r.intensity).filter((v): v is number => typeof v === "number")),
      2,
    ),
  };

  return [...generationRows, ...marketRows, ...emissionsRows, networkSummaryRow];
}

function aggregateRollupRows(rows: RollupRow[], bucket: string, network: NetworkCode): RollupRow[] {
  const groups = new Map<string, RollupRow[]>();

  rows.forEach((row) => {
    const key = `${row.region ?? ""}|${row.fueltech ?? ""}`;
    const existing = groups.get(key) ?? [];
    existing.push(row);
    groups.set(key, existing);
  });

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
        average(groupRows.map((row) => row.avg_power_mw).filter((v): v is number => typeof v === "number")),
        1,
      ),
      avg_price_per_mwh: round(
        average(groupRows.map((row) => row.avg_price_per_mwh).filter((v): v is number => typeof v === "number")),
        2,
      ),
      avg_proportion_pct: round(
        average(groupRows.map((row) => row.avg_proportion_pct).filter((v): v is number => typeof v === "number")),
        1,
      ),
      total_energy_mwh: round(groupRows.reduce((total, row) => total + Number(row.total_energy_mwh ?? 0), 0), 1),
      avg_price_dollar_per_mwh: round(
        average(
          groupRows.map((row) => row.avg_price_dollar_per_mwh).filter((v): v is number => typeof v === "number"),
        ),
        2,
      ),
      avg_demand_mw: round(
        average(groupRows.map((row) => row.avg_demand_mw).filter((v): v is number => typeof v === "number")),
        1,
      ),
      avg_renewables_pct: round(
        average(groupRows.map((row) => row.avg_renewables_pct).filter((v): v is number => typeof v === "number")),
        1,
      ),
      avg_net_generation_mw: round(
        average(groupRows.map((row) => row.avg_net_generation_mw).filter((v): v is number => typeof v === "number")),
        1,
      ),
      avg_renewables_mw: round(
        average(groupRows.map((row) => row.avg_renewables_mw).filter((v): v is number => typeof v === "number")),
        1,
      ),
      total_emissions_tco2e: round(groupRows.reduce((total, row) => total + Number(row.total_emissions_tco2e ?? 0), 0), 1),
      avg_intensity_kgco2e_per_mwh: round(
        average(
          groupRows
            .map((row) => row.avg_intensity_kgco2e_per_mwh)
            .filter((v): v is number => typeof v === "number"),
        ),
        2,
      ),
    };
  });
}

async function buildNetworkRollups(network: NetworkCode, previousHour: Date): Promise<void> {
  const rawKeys = buildHourlyRawKeys(network, previousHour);
  const snapshots = (await getJsonMany<EnergySnapshot>(rawKeys)).filter((snapshot): snapshot is EnergySnapshot => snapshot !== null);

  console.log(`Rollup builder found ${snapshots.length}/${rawKeys.length} raw snapshots for ${network}.`);

  if (snapshots.length === 0) {
    return;
  }

  const bucket = previousHour.toISOString().replace(/\.\d{3}Z$/, "Z");
  const hourlyRows = aggregateSnapshotsToRollupRows(snapshots, bucket, network);

  await putNdjson(
    getHourlyRollupKey(network, previousHour.getUTCFullYear(), previousHour.getUTCMonth() + 1, previousHour.getUTCDate(), previousHour.getUTCHours()),
    hourlyRows,
  );

  if (previousHour.getUTCHours() !== 23) {
    return;
  }

  const yesterday = new Date(previousHour);
  yesterday.setUTCHours(0, 0, 0, 0);

  const hourlyKeys = Array.from({ length: 24 }, (_, hour) =>
    getHourlyRollupKey(network, yesterday.getUTCFullYear(), yesterday.getUTCMonth() + 1, yesterday.getUTCDate(), hour),
  );

  const dailyRows = await getNdjsonMany<RollupRow>(hourlyKeys);
  if (dailyRows.length === 0) {
    return;
  }

  await putNdjson(
    getDailyRollupKey(network, yesterday.getUTCFullYear(), yesterday.getUTCMonth() + 1, yesterday.getUTCDate()),
    aggregateRollupRows(dailyRows, yesterday.toISOString().slice(0, 10), network),
  );
}

export async function handler(_event: unknown): Promise<{ statusCode: number }> {
  try {
    const previousHour = getPreviousHour();

    await Promise.all([buildNetworkRollups("NEM", previousHour), buildNetworkRollups("WEM", previousHour)]);
  } catch (error) {
    console.error("Rollup builder failed.", error);
  }

  return { statusCode: 200 };
}

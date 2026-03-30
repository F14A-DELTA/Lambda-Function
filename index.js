import { Client } from "@gradio/client";

// ===== CONFIG =====
const SPACE_NAME = "a13awd/electricity_grid_model";
const ENDPOINT = "/refresh_dashboard_1";

// thresholds (tune if needed)
const MAX_REASONABLE_PRICE = 10000;
const MAX_PERCENT_CHANGE = 200;

// ===== MAIN HANDLER =====
export const handler = async (event) => {
  const tests = [];
  let client;
  let result;

  try {
    // =========================
    // 1. API AVAILABILITY TEST
    // =========================
    client = await Client.connect(SPACE_NAME);
    result = await client.predict(ENDPOINT, []);

    if (!result || !result.data || result.data.length !== 3) {
      throw new Error("Invalid API response structure");
    }

    tests.push(pass("API availability"));

  } catch (err) {
    tests.push(fail("API availability", err.message));
    return buildResponse("FAIL", tests);
  }

  let summary, tableData;

  try {
    [summary, tableData] = result.data;

    // =========================
    // 2. SCHEMA VALIDATION TEST
    // =========================
    if (typeof summary !== "string") {
      throw new Error("Summary is not a string");
    }

    // The Gradio component returns { headers: [...], data: [...] }
    const rows = Array.isArray(tableData) ? tableData : tableData?.data;
    
    if (!Array.isArray(rows)) {
      throw new Error("Table data is not an array");
    }

    // Map the raw data from positional array to named object
    // Based on inspection: 0: Region, 1: Current Price, 2: 5m, 3: 15m, 4: 30m
    const table = rows.map(row => ({
      Region: row[0],
      "Current Price": row[1],
      "Predicted Price In 5m": row[2],
      "Predicted Price In 15m": row[3],
      "Predicted Price In 30m": row[4]
    }));

    const requiredFields = [
      "Region",
      "Current Price",
      "Predicted Price In 5m",
      "Predicted Price In 15m",
      "Predicted Price In 30m"
    ];

    const firstRow = table[0];
    for (const field of requiredFields) {
      if (firstRow[field] === undefined) {
        throw new Error(`Missing field or data: ${field}`);
      }
    }

    tests.push(pass("Schema validation"));

    // =========================
    // 3. VALUE SANITY TEST
    // =========================
    for (const row of table) {
      const values = [
        row["Current Price"],
        row["Predicted Price In 5m"],
        row["Predicted Price In 15m"],
        row["Predicted Price In 30m"]
      ];

      for (const val of values) {
        if (val === null || val === undefined || isNaN(val)) {
          throw new Error(`Invalid number detected in region ${row.Region}`);
        }

        if (val > MAX_REASONABLE_PRICE) {
          throw new Error(`Unrealistic spike in ${row.Region}: ${val}`);
        }

        if (val < -1000) {
          throw new Error(`Extreme negative price in ${row.Region}: ${val}`);
        }
      }
    }
    tests.push(pass("Value sanity"));

    // =========================
    // 4. CONSISTENCY TEST
    // =========================
    for (const row of table) {
      const current = row["Current Price"];
      const p5 = row["Predicted Price In 5m"];
      const p15 = row["Predicted Price In 15m"];
      const p30 = row["Predicted Price In 30m"];

      const changes = [
        percentChange(current, p5),
        percentChange(p5, p15),
        percentChange(p15, p30)
      ];

      for (const change of changes) {
        if (Math.abs(change) > MAX_PERCENT_CHANGE) {
          throw new Error(
            `Unstable prediction in ${row.Region}: ${change.toFixed(2)}%`
          );
        }
      }
    }
    tests.push(pass("Consistency / drift"));

    // =========================
    // 5. REGRESSION TEST
    // =========================
    if (event?.baseline) {
      const baseline = event.baseline;
      for (const row of table) {
        const region = row.Region;
        if (baseline[region]) {
          const oldVal = baseline[region]["Predicted Price In 5m"];
          const newVal = row["Predicted Price In 5m"];
          const diff = Math.abs(percentChange(oldVal, newVal));
          if (diff > MAX_PERCENT_CHANGE) {
            throw new Error(`Regression drift in ${region}: ${diff.toFixed(2)}%`);
          }
        }
      }
    }
    tests.push(pass("Regression test"));

    // FINAL SUCCESS RESULT
    const overall = tests.some(t => t.status === "FAIL") ? "FAIL" : "PASS";
    return buildResponse(overall, tests, {
      summary,
      sample: table.slice(0, 2)
    });

  } catch (err) {
    tests.push(fail("Processing", err.message));
    return buildResponse("FAIL", tests);
  }
};

// ===== HELPERS =====

function percentChange(a, b) {
  if (a === 0) return 0;
  return ((b - a) / Math.abs(a)) * 100;
}

function pass(name) {
  return { name, status: "PASS" };
}

function fail(name, message) {
  return { name, status: "FAIL", message };
}

function buildResponse(status, tests, extra = {}) {
  return {
    statusCode: 200,
    body: JSON.stringify({
      status,
      timestamp: new Date().toISOString(),
      tests,
      ...extra
    })
  };
}

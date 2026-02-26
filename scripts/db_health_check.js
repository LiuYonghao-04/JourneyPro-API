import { pool } from "../db/connect.js";

const MB = 1024 * 1024;

const toMb = (n) => (Number(n || 0) / MB).toFixed(1);
const pick = (row, ...keys) => {
  for (const key of keys) {
    if (row && Object.prototype.hasOwnProperty.call(row, key)) return row[key];
  }
  return undefined;
};

const printSection = (title) => {
  console.log("");
  console.log(`=== ${title} ===`);
};

async function main() {
  const [tableRows] = await pool.query(
    `
      SELECT
        table_name,
        table_rows,
        data_length,
        index_length,
        (data_length + index_length) AS total_bytes
      FROM information_schema.tables
      WHERE table_schema = DATABASE()
      ORDER BY total_bytes DESC
    `
  );

  printSection("Top Tables By Size");
  tableRows.slice(0, 12).forEach((row, idx) => {
    const tableName = pick(row, "table_name", "TABLE_NAME");
    const tableRowsCount = Number(pick(row, "table_rows", "TABLE_ROWS") || 0);
    const dataLen = pick(row, "data_length", "DATA_LENGTH");
    const indexLen = pick(row, "index_length", "INDEX_LENGTH");
    const totalBytes = pick(row, "total_bytes", "TOTAL_BYTES");
    const dataMb = toMb(dataLen);
    const indexMb = toMb(indexLen);
    const totalMb = toMb(totalBytes);
    console.log(
      `${idx + 1}. ${tableName} rows=${tableRowsCount} data=${dataMb}MB index=${indexMb}MB total=${totalMb}MB`
    );
  });

  const [indexRows] = await pool.query(
    `
      SELECT
        table_name,
        index_name,
        GROUP_CONCAT(column_name ORDER BY seq_in_index) AS columns,
        non_unique
      FROM information_schema.statistics
      WHERE table_schema = DATABASE()
      GROUP BY table_name, index_name, non_unique
      ORDER BY table_name ASC, index_name ASC
    `
  );

  const indexMap = new Map();
  indexRows.forEach((row) => {
    const tableName = pick(row, "table_name", "TABLE_NAME");
    if (!indexMap.has(tableName)) indexMap.set(tableName, []);
    indexMap.get(tableName).push(row);
  });

  printSection("Tables With Many Indexes");
  [...indexMap.entries()]
    .map(([table, list]) => ({ table, count: list.length }))
    .filter((item) => item.count >= 10)
    .sort((a, b) => b.count - a.count)
    .forEach((item) => console.log(`${item.table}: ${item.count} indexes`));

  const needsBackfill = [];
  const lookup = Object.fromEntries(
    tableRows.map((r) => [pick(r, "table_name", "TABLE_NAME"), Number(pick(r, "table_rows", "TABLE_ROWS") || 0)])
  );
  if ((lookup.user_interest_agg || 0) === 0) needsBackfill.push("user_interest_agg is empty -> run `npm run reco:offline`");
  if ((lookup.poi_quality_stats || 0) === 0) needsBackfill.push("poi_quality_stats is empty -> run `npm run reco:offline`");
  if ((lookup.recommendation_events || 0) < 1000)
    needsBackfill.push("recommendation_events is small -> verify frontend event logging and backfill");

  printSection("Action Hints");
  if (!needsBackfill.length) {
    console.log("No immediate backfill blockers detected.");
  } else {
    needsBackfill.forEach((line) => console.log(`- ${line}`));
  }

  const largest = tableRows[0];
  if (largest) {
    const tableName = pick(largest, "table_name", "TABLE_NAME");
    const totalBytes = pick(largest, "total_bytes", "TOTAL_BYTES");
    printSection("Largest Table Focus");
    console.log(
      `Largest table is ${tableName} (${toMb(totalBytes)}MB). Consider archive/partition strategy if growth continues.`
    );
  }
}

main()
  .catch((err) => {
    console.error("db health check failed:", err?.message || err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });

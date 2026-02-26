import "dotenv/config";
import fs from "fs/promises";
import path from "path";
import { spawn } from "child_process";
import { pool } from "../db/connect.js";

const args = process.argv.slice(2);
const arg = (name, fallback = "") => {
  const hit = args.find((item) => item.startsWith(`--${name}=`));
  return hit ? hit.slice(name.length + 3) : fallback;
};

const modeRaw = String(arg("mode", "snapshot")).trim().toLowerCase();
const mode = new Set(["snapshot", "schema", "critical", "full"]).has(modeRaw) ? modeRaw : "snapshot";
const dbName = process.env.DB_NAME || "journeypro";
const now = new Date();
const tag = `${now.getFullYear()}${String(now.getMonth() + 1).padStart(2, "0")}${String(now.getDate()).padStart(2, "0")}_${String(now.getHours()).padStart(2, "0")}${String(now.getMinutes()).padStart(2, "0")}${String(now.getSeconds()).padStart(2, "0")}`;
const outRoot = path.resolve(process.cwd(), arg("out", `backups/${tag}`));

const CRITICAL_TABLES = [
  "users",
  "user_notification_state",
  "user_follows",
  "user_recommendation_settings",
  "ab_assignments",
  "recommendation_events",
];

const qTable = (tableName) => `\`${String(tableName || "").replace(/`/g, "``")}\``;

async function mkdirSafe(targetDir) {
  await fs.mkdir(targetDir, { recursive: true });
}

async function listTables() {
  const [rows] = await pool.query(
    `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
      ORDER BY table_name ASC
    `,
    [dbName]
  );
  return rows
    .map((row) => String(row.table_name ?? row.TABLE_NAME ?? "").trim())
    .filter(Boolean);
}

async function tableStats() {
  const [rows] = await pool.query(
    `
      SELECT
        table_name,
        table_rows,
        data_length,
        index_length,
        (data_length + index_length) AS total_length
      FROM information_schema.tables
      WHERE table_schema = ?
      ORDER BY total_length DESC, table_name ASC
    `,
    [dbName]
  );
  return rows.map((row) => ({
    table_name: String(row.table_name ?? row.TABLE_NAME ?? ""),
    table_rows: Number(row.table_rows ?? row.TABLE_ROWS ?? 0),
    data_length: Number(row.data_length ?? row.DATA_LENGTH ?? 0),
    index_length: Number(row.index_length ?? row.INDEX_LENGTH ?? 0),
    total_length: Number(row.total_length ?? row.TOTAL_LENGTH ?? 0),
  }));
}

async function dumpSchema(tables) {
  const outPath = path.join(outRoot, "schema.sql");
  const lines = [];
  lines.push(`-- JourneyPro schema snapshot`);
  lines.push(`-- database: ${dbName}`);
  lines.push(`-- generated_at: ${new Date().toISOString()}`);
  lines.push("");
  for (const table of tables) {
    // eslint-disable-next-line no-await-in-loop
    const [rows] = await pool.query(`SHOW CREATE TABLE ${qTable(table)}`);
    const createSql = rows?.[0]?.["Create Table"];
    if (!createSql) continue;
    lines.push(`-- table: ${table}`);
    lines.push(`DROP TABLE IF EXISTS ${qTable(table)};`);
    lines.push(`${createSql};`);
    lines.push("");
  }
  await fs.writeFile(outPath, lines.join("\n"), "utf8");
  return outPath;
}

async function dumpTableNdjson(tableName, rowLimit = 500000) {
  const outPath = path.join(outRoot, `${tableName}.ndjson`);
  const rowsPerBatch = 5000;
  let wrote = 0;
  let lastId = 0;

  const [[pkRow]] = await pool.query(
    `
      SELECT k.COLUMN_NAME AS column_name, c.DATA_TYPE AS data_type
      FROM information_schema.table_constraints t
      JOIN information_schema.key_column_usage k
        ON k.constraint_name = t.constraint_name
       AND k.table_schema = t.table_schema
       AND k.table_name = t.table_name
      JOIN information_schema.columns c
        ON c.table_schema = k.table_schema
       AND c.table_name = k.table_name
       AND c.column_name = k.column_name
      WHERE t.table_schema = ?
        AND t.table_name = ?
        AND t.constraint_type = 'PRIMARY KEY'
      ORDER BY k.ordinal_position
      LIMIT 1
    `,
    [dbName, tableName]
  );

  const pk = pkRow?.column_name ? String(pkRow.column_name) : "";
  const pkType = String(pkRow?.data_type || "").toLowerCase();
  const hasNumericPk = !!pk && /(int|bigint|smallint|mediumint|tinyint|decimal|numeric)/.test(pkType);
  const content = [];

  if (!hasNumericPk) {
    const [rows] = await pool.query(`SELECT * FROM ${qTable(tableName)} LIMIT ?`, [rowLimit]);
    rows.forEach((row) => content.push(JSON.stringify(row)));
    await fs.writeFile(outPath, `${content.join("\n")}${content.length ? "\n" : ""}`, "utf8");
    return { outPath, rows: rows.length, truncated: rows.length >= rowLimit };
  }

  while (wrote < rowLimit) {
    const remaining = Math.max(0, rowLimit - wrote);
    const take = Math.min(rowsPerBatch, remaining);
    // eslint-disable-next-line no-await-in-loop
    const [rows] = await pool.query(
      `SELECT * FROM ${qTable(tableName)} WHERE ${qTable(pk)} > ? ORDER BY ${qTable(pk)} ASC LIMIT ?`,
      [lastId, take]
    );
    if (!rows.length) break;
    rows.forEach((row) => content.push(JSON.stringify(row)));
    wrote += rows.length;
    lastId = Number(rows[rows.length - 1]?.[pk] || lastId);
    if (!Number.isFinite(lastId)) break;
  }

  await fs.writeFile(outPath, `${content.join("\n")}${content.length ? "\n" : ""}`, "utf8");
  return { outPath, rows: wrote, truncated: wrote >= rowLimit };
}

async function dumpCritical(existingTables) {
  const hits = CRITICAL_TABLES.filter((table) => existingTables.has(table));
  const results = [];
  for (const table of hits) {
    // eslint-disable-next-line no-await-in-loop
    const result = await dumpTableNdjson(table, 500000);
    results.push({ table, ...result });
  }
  return results;
}

async function dumpFullViaMysqldump() {
  const outPath = path.join(outRoot, `${dbName}_full.sql`);
  return new Promise((resolve, reject) => {
    const child = spawn(
      "mysqldump",
      [
        "--single-transaction",
        "--quick",
        "--routines",
        "--events",
        "--triggers",
        "--databases",
        dbName,
      ],
      {
        env: {
          ...process.env,
          MYSQL_PWD: process.env.DB_PASS || "",
        },
        windowsHide: true,
      }
    );

    let stderr = "";
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk || "");
    });

    const chunks = [];
    child.stdout.on("data", (chunk) => chunks.push(chunk));
    child.on("error", (err) => {
      reject(new Error(`mysqldump start failed: ${err?.message || err}`));
    });
    child.on("close", async (code) => {
      if (code !== 0) {
        reject(new Error(`mysqldump exit=${code} ${stderr.trim()}`));
        return;
      }
      try {
        await fs.writeFile(outPath, Buffer.concat(chunks));
        resolve(outPath);
      } catch (e) {
        reject(e);
      }
    });
  });
}

async function main() {
  console.log(`[backup] start mode=${mode} db=${dbName}`);
  console.log(`[backup] output=${outRoot}`);
  await mkdirSafe(outRoot);

  const tables = await listTables();
  const tableSet = new Set(tables);
  const stats = await tableStats();
  await fs.writeFile(path.join(outRoot, "table_stats.json"), JSON.stringify(stats, null, 2), "utf8");

  const manifest = {
    mode,
    db: dbName,
    generated_at: new Date().toISOString(),
    table_count: tables.length,
    outputs: {},
  };

  if (mode === "schema" || mode === "snapshot" || mode === "full") {
    const schemaPath = await dumpSchema(tables);
    manifest.outputs.schema = path.relative(outRoot, schemaPath);
    console.log(`[backup] schema -> ${schemaPath}`);
  }

  if (mode === "critical" || mode === "snapshot" || mode === "full") {
    const critical = await dumpCritical(tableSet);
    manifest.outputs.critical = critical.map((row) => ({
      table: row.table,
      file: path.relative(outRoot, row.outPath),
      rows: row.rows,
      truncated: !!row.truncated,
    }));
    console.log(`[backup] critical tables dumped: ${critical.length}`);
  }

  if (mode === "full") {
    try {
      const fullPath = await dumpFullViaMysqldump();
      manifest.outputs.full = path.relative(outRoot, fullPath);
      console.log(`[backup] full -> ${fullPath}`);
    } catch (e) {
      manifest.outputs.full_error = String(e?.message || e);
      console.warn(`[backup] full dump skipped/failed: ${manifest.outputs.full_error}`);
    }
  }

  await fs.writeFile(path.join(outRoot, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");
  console.log("[backup] done");
}

main()
  .catch((err) => {
    console.error("[backup] failed", err);
    process.exitCode = 1;
  })
  .finally(async () => {
    try {
      await pool.end();
    } catch {
      // ignore
    }
  });

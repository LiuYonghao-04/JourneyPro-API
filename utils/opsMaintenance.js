import fs from "fs/promises";
import path from "path";
import { execFile } from "child_process";
import { promisify } from "util";
import { fileURLToPath } from "url";

const execFileAsync = promisify(execFile);
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const API_ROOT = path.resolve(__dirname, "..");
const BACKUP_ROOT = path.join(API_ROOT, "backups");
const MAINTENANCE_LOG_ROOT = path.join(API_ROOT, "logs", "maintenance");
const TASK_PREFIX = "JourneyPro";
const DEFAULT_BACKUP_FRESH_HOURS = 48;
const DEFAULT_LOG_TAIL_LINES = 12;

const TASK_DEFS = [
  {
    key: "db_maintenance_daily",
    task_name: `${TASK_PREFIX}-DBMaintenance-Daily`,
    label: "DB maintenance + backup",
  },
  {
    key: "comments_archive_hourly",
    task_name: `${TASK_PREFIX}-CommentsArchive-Hourly`,
    label: "Comments archive",
  },
];

const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const toIso = (value) => {
  if (!value) return null;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
};

async function readJson(filePath) {
  const raw = await fs.readFile(filePath, "utf8");
  return JSON.parse(raw);
}

async function readLatestBackupManifest() {
  let dirents = [];
  try {
    dirents = await fs.readdir(BACKUP_ROOT, { withFileTypes: true });
  } catch {
    return null;
  }

  const manifests = [];
  for (const dirent of dirents) {
    if (!dirent.isDirectory()) continue;
    const folderName = String(dirent.name || "").trim();
    if (!folderName || folderName === "social_rebalance") continue;
    const manifestPath = path.join(BACKUP_ROOT, folderName, "manifest.json");
    try {
      // eslint-disable-next-line no-await-in-loop
      const stats = await fs.stat(manifestPath);
      manifests.push({
        folder_name: folderName,
        manifest_path: manifestPath,
        manifest_mtime: stats.mtime,
      });
    } catch {
      // ignore folders without manifest
    }
  }

  if (!manifests.length) return null;
  manifests.sort((a, b) => b.manifest_mtime.getTime() - a.manifest_mtime.getTime());

  const latest = manifests[0];
  const manifest = await readJson(latest.manifest_path);
  const generatedAt = toIso(manifest?.generated_at || latest.manifest_mtime);
  const ageHours =
    generatedAt != null ? Math.max(0, (Date.now() - new Date(generatedAt).getTime()) / (1000 * 60 * 60)) : null;
  const criticalOutputs = Array.isArray(manifest?.outputs?.critical) ? manifest.outputs.critical : [];
  return {
    folder_name: latest.folder_name,
    manifest_path: latest.manifest_path,
    generated_at: generatedAt,
    age_hours: ageHours != null ? Number(ageHours.toFixed(2)) : null,
    is_fresh: ageHours != null ? ageHours <= DEFAULT_BACKUP_FRESH_HOURS : false,
    mode: String(manifest?.mode || "snapshot"),
    table_count: safeNumber(manifest?.table_count),
    schema_file: manifest?.outputs?.schema || null,
    critical_table_count: criticalOutputs.length,
    critical_tables: criticalOutputs,
    has_truncated_dump: criticalOutputs.some((item) => !!item?.truncated),
  };
}

async function readLatestLog(prefix, tailLines = DEFAULT_LOG_TAIL_LINES) {
  let dirents = [];
  try {
    dirents = await fs.readdir(MAINTENANCE_LOG_ROOT, { withFileTypes: true });
  } catch {
    return null;
  }

  const hits = dirents
    .filter((dirent) => dirent.isFile() && String(dirent.name || "").startsWith(prefix))
    .map((dirent) => dirent.name);
  if (!hits.length) return null;

  let latest = null;
  for (const fileName of hits) {
    const filePath = path.join(MAINTENANCE_LOG_ROOT, fileName);
    try {
      // eslint-disable-next-line no-await-in-loop
      const stats = await fs.stat(filePath);
      if (!latest || stats.mtimeMs > latest.mtimeMs) {
        latest = {
          file_name: fileName,
          file_path: filePath,
          mtimeMs: stats.mtimeMs,
          size_bytes: stats.size,
        };
      }
    } catch {
      // ignore
    }
  }

  if (!latest) return null;
  const buffer = await fs.readFile(latest.file_path);
  const utf8Raw = buffer.toString("utf8");
  const nulCount = (utf8Raw.match(/\u0000/g) || []).length;
  const raw = nulCount > Math.max(4, Math.floor(utf8Raw.length / 20)) ? buffer.toString("utf16le") : utf8Raw;
  const lines = raw
    .split(/\r?\n/)
    .map((line) => line.trimEnd())
    .filter(Boolean);
  return {
    file_name: latest.file_name,
    last_write_at: toIso(new Date(latest.mtimeMs)),
    size_bytes: latest.size_bytes,
    tail: lines.slice(-Math.max(1, tailLines)),
  };
}

async function queryScheduledTask(taskName) {
  if (process.platform !== "win32") {
    return {
      installed: false,
      supported: false,
      message: "Scheduled task inspection is only available on Windows.",
    };
  }

  const escapedTaskName = String(taskName || "").replace(/'/g, "''");
  const command = [
    "-NoProfile",
    "-Command",
    [
      "$ErrorActionPreference = 'Stop'",
      `$task = Get-ScheduledTask -TaskName '${escapedTaskName}' -ErrorAction Stop`,
      `$info = Get-ScheduledTaskInfo -TaskName '${escapedTaskName}' -ErrorAction Stop`,
      "$payload = [pscustomobject]@{ " +
        "task_name = $task.TaskName; " +
        "task_path = $task.TaskPath; " +
        "state = [string]$task.State; " +
        "enabled = [bool]$task.Settings.Enabled; " +
        "last_run_time = if ($info.LastRunTime -and $info.LastRunTime.Year -gt 1900) { $info.LastRunTime.ToString('o') } else { $null }; " +
        "next_run_time = if ($info.NextRunTime -and $info.NextRunTime.Year -gt 1900) { $info.NextRunTime.ToString('o') } else { $null }; " +
        "last_result = [int]$info.LastTaskResult; " +
        "missed_runs = [int]$info.NumberOfMissedRuns }",
      "$payload | ConvertTo-Json -Compress -Depth 4",
    ].join("; "),
  ];

  try {
    const { stdout } = await execFileAsync("powershell.exe", command, {
      timeout: 8000,
      windowsHide: true,
      maxBuffer: 1024 * 1024,
    });
    const parsed = JSON.parse(String(stdout || "{}").trim() || "{}");
    const lastRunTime = toIso(parsed.last_run_time);
    const hasRealLastRun = !!lastRunTime && new Date(lastRunTime).getUTCFullYear() > 2000;
    const lastResult = hasRealLastRun ? safeNumber(parsed.last_result) : null;
    return {
      installed: true,
      supported: true,
      task_name: parsed.task_name || taskName,
      task_path: parsed.task_path || "\\",
      state: parsed.state || "Unknown",
      enabled: !!parsed.enabled,
      last_run_time: hasRealLastRun ? lastRunTime : null,
      next_run_time: toIso(parsed.next_run_time),
      last_result: lastResult,
      missed_runs: safeNumber(parsed.missed_runs),
      last_result_ok: lastResult == null ? null : lastResult === 0,
    };
  } catch (err) {
    const message = String(err?.stderr || err?.stdout || err?.message || err);
    if (/cannot find|not find|cannot find the file/i.test(message)) {
      return {
        installed: false,
        supported: true,
        message: "Task not installed.",
      };
    }
    return {
      installed: false,
      supported: true,
      message: message.trim() || "Task inspection failed.",
    };
  }
}

export async function fetchOpsMaintenanceStatus({ freshHours = DEFAULT_BACKUP_FRESH_HOURS } = {}) {
  const [backup, dbLog, archiveLog, ...taskResults] = await Promise.all([
    readLatestBackupManifest(),
    readLatestLog("db_maintenance_daily_"),
    readLatestLog("comments_archive_hot_"),
    ...TASK_DEFS.map((task) => queryScheduledTask(task.task_name)),
  ]);

  const latestBackup =
    backup == null
      ? null
      : {
          ...backup,
          is_fresh:
            backup.age_hours != null ? Number(backup.age_hours) <= Math.max(1, safeNumber(freshHours, 48)) : false,
        };

  const tasks = TASK_DEFS.map((task, index) => ({
    key: task.key,
    label: task.label,
    task_name: task.task_name,
    ...(taskResults[index] || {}),
  }));

  return {
    backup_fresh_hours: Math.max(1, safeNumber(freshHours, 48)),
    latest_backup: latestBackup,
    auto_backup_ready: !!latestBackup?.is_fresh && !!tasks.find((task) => task.key === "db_maintenance_daily")?.enabled,
    tasks,
    logs: {
      db_maintenance: dbLog,
      comments_archive: archiveLog,
    },
    install_hint: "npm run ops:schedule:install",
  };
}

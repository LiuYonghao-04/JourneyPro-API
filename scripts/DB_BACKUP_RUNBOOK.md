# JourneyPro DB Backup Runbook

## 1) Safety principle

- No destructive SQL is included in this runbook.
- Always run a snapshot backup before index/archiving maintenance.
- Keep at least one backup from the current day before release.

## 2) Backup commands

Run in `JourneyPro-api`:

```bash
npm run db:backup:snapshot
```

Schema only:

```bash
npm run db:backup:schema
```

Critical data only:

```bash
npm run db:backup:critical
```

Output folder:

- `JourneyPro-api/backups/<timestamp>/manifest.json`
- `JourneyPro-api/backups/<timestamp>/schema.sql`
- `JourneyPro-api/backups/<timestamp>/*.ndjson` (critical tables)

## 3) Optional full dump (best effort)

```bash
node scripts/db_backup_snapshot.js --mode=full
```

Notes:

- Requires `mysqldump` available in PATH.
- If `mysqldump` is missing, the script still keeps schema + critical export.

## 4) Restore guidance (manual)

1. Restore schema from `schema.sql` into an empty database.
2. Import critical table files (`*.ndjson`) with a custom import script or SQL client.
3. Validate row counts and random sample records before switching traffic.

## 5) Release gate

Before release:

1. `npm run db:backup:snapshot`
2. `npm run release:check`
3. If `FAIL > 0`, do not release.

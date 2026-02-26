# JourneyPro API

JourneyPro API is the Node.js backend for the JourneyPro graduation project. It provides authentication, community features (posts / likes / favorites / comments / follow / notifications / chat), POI search, and a personalized along-route recommendation endpoint that combines user interest signals with route proximity.

## Table of contents

- [Highlights](#highlights)
- [Tech stack](#tech-stack)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [API overview](#api-overview)
- [Database notes](#database-notes)
- [Data import scripts](#data-import-scripts)
- [Maintenance scheduler](#maintenance-scheduler)
- [Release hardening](#release-hardening)
- [Security notes](#security-notes)

## Highlights

- JWT-based auth + bcrypt password hashing + captcha
- Community module: posts, tags, likes, favorites, nested comments, follow relationships
- Notifications:
  - REST list API
  - SSE stream (`/api/notifications/stream`) for real-time updates
- Chat (basic DM) with notification push
- Image upload (multipart) + safe image proxy (SSRF-protected, CORS-friendly for the frontend cropper)
- Route endpoints:
  - `GET /api/route/with-poi` for routing with a selected POI
  - `GET /api/route/recommend` for along-route POI recommendation (interest vs distance tuning affects ordering only)
- Recommendation profile endpoint:
  - `GET /api/recommendation/profile` returns interest distribution (tags/categories) as percentages
  - `GET/POST /api/recommendation/settings` stores user tuning (interest vs distance)

## Tech stack

- Node.js (ESM)
- Express
- MySQL (`mysql2`)
- Turf.js (geometry utilities)
- Multer (multipart upload)
- Axios (HTTP)

## Getting started

### Prerequisites

- Node.js 18+
- MySQL 8+ (recommended)
- OSRM instance (local or remote) for routing and along-route recommendation

### Install

```bash
cd JourneyPro-api
npm install
```

### Configure env

Copy `JourneyPro-api/.env.example` to `JourneyPro-api/.env` and adjust values:

```bash
PORT=3001
DB_HOST=localhost
DB_PORT=3306
DB_USER=root
DB_PASS=123456
DB_NAME=journeypro
JWT_SECRET=replace-me
OSRM_URL=http://localhost:5000
OSRM_URLS=http://localhost:5000,https://router.project-osrm.org
OSRM_ENABLE_PUBLIC_FALLBACK=1
OSRM_LOCAL_TIMEOUT_MS=600
OSRM_REMOTE_TIMEOUT_MS=12000
OSRM_DOWN_COOLDOWN_MS=15000
```

### Run

```bash
npm run dev
```

Server will listen on `http://localhost:3001` by default.

## Configuration

| Name | Default | Description |
| --- | --- | --- |
| `PORT` | `3001` | API server port |
| `DB_HOST` | `localhost` | MySQL host |
| `DB_PORT` | `3306` | MySQL port |
| `DB_USER` | `root` | MySQL user |
| `DB_PASS` | `123456` | MySQL password (change this in production) |
| `DB_NAME` | `journeypro` | Database name |
| `DB_POOL_SIZE` | `10` | MySQL connection pool size |
| `JWT_SECRET` | `journeypro-secret` | JWT signing secret |
| `OSRM_URL` | `http://localhost:5000` | Single OSRM backend (legacy option, local preferred) |
| `OSRM_URLS` | `http://localhost:5000,https://router.project-osrm.org` | Ordered OSRM backend list (local first, online fallback) |
| `OSRM_ENABLE_PUBLIC_FALLBACK` | `1` | Auto-append `https://router.project-osrm.org` when missing |
| `OSRM_LOCAL_TIMEOUT_MS` | `600` | Timeout for local/private OSRM backends |
| `OSRM_REMOTE_TIMEOUT_MS` | `12000` | Timeout for public/remote OSRM backends |
| `OSRM_DOWN_COOLDOWN_MS` | `15000` | Circuit-break cooldown after backend failure |
| `ENABLE_RUNTIME_SCHEMA_MIGRATION` | `0` | `1` enables route-time DDL; keep `0` in release |
| `SLOW_API_MS` | `800` | Slow API warning threshold |
| `METRIC_SAMPLE_LIMIT` | `400` | In-memory latency sample count per endpoint |
| `OPS_METRICS_TOKEN` | _(empty)_ | Optional token for `/api/ops/*` endpoints |
| `BANDIT_HISTORY_CACHE_TTL_MS` | `120000` | In-memory cache TTL for bandit arm history |
| `BANDIT_USE_EVENT_HISTORY` | `0` | `1` to query `recommendation_events` directly (slower on large tables) |

## API overview

Base path: `/api`

### Auth

- `GET /api/auth/captcha`
- `POST /api/auth/register`
- `POST /api/auth/login`
- `GET /api/auth/user?id=123`
- `POST /api/auth/avatar`

### Posts & community

- `GET /api/posts?limit=20&offset=0&sort=latest|hot`
- `GET /api/posts/:id`
- `POST /api/posts`
- `POST /api/posts/:id/like`
- `POST /api/posts/:id/favorite`
- `GET /api/posts/tags/list`
- `GET /api/posts/:id/comments`
- `POST /api/posts/:id/comments`
- `POST /api/posts/comments/:cid/like`

### POI

- `GET /api/poi/search?q=keyword`
- `GET /api/poi/nearby?lat=..&lng=..&radius=..`
- `GET /api/poi/:id`

### Route

- `GET /api/route/with-poi?start=lng,lat&poi=lng,lat&end=lng,lat`
- `GET /api/route/recommend?start=lng,lat&end=lng,lat&via=lng,lat;lng,lat&user_id=1&interest_weight=0.5`

### Recommendation settings / profile

- `GET /api/recommendation/settings?user_id=1`
- `POST /api/recommendation/settings`
- `GET /api/recommendation/profile?user_id=1`

### Follow

- `GET /api/follow/status?user_id=1&target_id=2`
- `POST /api/follow/toggle`
- `GET /api/follow/followers?target_id=1`

### Notifications

- `GET /api/notifications?user_id=1`
- `GET /api/notifications?user_id=1&before_ts=...&before_id=...` (older page cursor)
- `GET /api/notifications/stream?user_id=1` (SSE)

### Ops / observability

- `GET /api/ops/health`
- `GET /api/ops/metrics`

### Chat

- `GET /api/chat/list?user_id=1`
- `GET /api/chat/history?user_id=1&peer_id=2`
- `GET /api/chat/search?keyword=abc&user_id=1`
- `POST /api/chat/send`

### Upload

- `POST /api/upload/image` (multipart field: `file`)
- `GET /api/upload/proxy?url=https%3A%2F%2F...` (image proxy with basic SSRF protection)

### Dev helpers

- `POST /api/dev/seed/posts` (demo seeding)

## Database notes

- The project uses a MySQL schema documented in `JourneyPro 社区模块数据库名称文档（数据字典）.md` (in the main project folder).
- Some routes will auto-create helper tables if they do not exist (e.g. notification/chat tables, recommendation settings).
- POI queries use MySQL spatial functions and expect a `poi.geom` column (SRID 4326) with a spatial index for performance.

## Data import scripts

Optional scripts (for building a POI dataset):

- `scripts/import_osm_poi_ndjson.js` (OSM NDJSON to `poi`)
- `scripts/import_naptan_stops_csv.js` (NaPTAN Stops CSV to `poi`)
- `scripts/import_tfl_stoppoints.js` (TfL StopPoint API to `poi`)

All scripts read database settings from the same env vars (`DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASS`, `DB_NAME`).

Example:

```bash
# OSM NDJSON import
NDJSON_PATH=/path/to/london-poi.ndjson node scripts/import_osm_poi_ndjson.js

# NaPTAN CSV import
NAPTAN_CSV_PATH=/path/to/Stops.csv node scripts/import_naptan_stops_csv.js

# TfL StopPoints import (optional key)
TFL_APP_KEY=your-key node scripts/import_tfl_stoppoints.js
```

## Maintenance scheduler

For production-like stability on large datasets, this repo now includes two scheduled maintenance jobs:

- Hourly `post_comments` hot-table archive sweep (incremental, capped by batch size)
- Daily DB health + redundant index compaction check/apply

### One-shot manual run

```bash
npm run ops:run:archive-hourly
npm run ops:run:db-daily
```

### Install Windows Task Scheduler jobs

```bash
npm run ops:schedule:install
```

Default installed tasks:

- `JourneyPro-CommentsArchive-Hourly` (hourly)
- `JourneyPro-DBMaintenance-Daily` (03:20 every day)

Customize install parameters (PowerShell):

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/scheduler/install_windows_tasks.ps1 `
  -TaskPrefix JourneyPro `
  -HourlyStart 00:10 `
  -DailyTime 03:20 `
  -RetainHotRows 10000000 `
  -ArchiveBatchSize 30000 `
  -ArchiveMaxBatches 3 `
  -OlderThanDays 0 `
  -ApplyIndexFix:$true
```

### Uninstall scheduler jobs

```bash
npm run ops:schedule:uninstall
```

### Logs

Maintenance logs are written to:

- `JourneyPro-api/logs/maintenance/comments_archive_hot_YYYYMMDD.log`
- `JourneyPro-api/logs/maintenance/db_maintenance_daily_YYYYMMDD.log`

## Release hardening

### Backup first (no-downtime safe snapshot)

```bash
npm run db:backup:snapshot
```

This writes:

- `backups/<timestamp>/manifest.json`
- `backups/<timestamp>/schema.sql`
- `backups/<timestamp>/*.ndjson` for critical tables

### Pre-release gates

```bash
npm run release:check
npm run loadtest:core -- --duration=30 --concurrency=8
```

### Runbooks

- Backup runbook: `scripts/DB_BACKUP_RUNBOOK.md`
- Go/No-Go checklist: `scripts/RELEASE_GONOGO_CHECKLIST.md`

## Security notes

- Do not expose `POST /api/dev/seed/posts` in public deployments.
- `JWT_SECRET` must be changed for production.
- The image proxy endpoint blocks private IP ranges and non-HTTP(S) schemes, but you should still keep it behind reasonable rate limits in production.

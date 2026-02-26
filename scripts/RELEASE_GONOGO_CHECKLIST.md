# JourneyPro V2 Beta Release Go/No-Go

## Mandatory gates

1. **Backup present**
- Run: `npm run db:backup:snapshot`
- Verify: latest `backups/<timestamp>/manifest.json` exists.

2. **Runtime migration disabled**
- Ensure env: `ENABLE_RUNTIME_SCHEMA_MIGRATION=0`

3. **Pre-release check**
- Run: `npm run release:check`
- Result must be: `FAIL=0`

4. **Core API load test**
- Run: `npm run loadtest:core -- --duration=30 --concurrency=8`
- Target:
  - Feed/Notifications/Post detail `P95 <= 1500ms`
  - Error rate `<= 0.5%`

5. **Ops endpoint available**
- `GET /api/ops/health`
- `GET /api/ops/metrics`

## Rollback readiness

1. Snapshot path is recorded.
2. DB rollback script available:
- `npm run db:rollback:index-v2`
- `npm run comments:archive:rollback`
3. Revert target commit hash is recorded in release note.

## Post-release watch (first 30 minutes)

1. Slow API logs (`[slow-api]`) do not spike.
2. `/api/ops/metrics`:
- `slow_api_count` growth is stable
- no endpoint with sustained `p95 > 2000ms`
3. Notification unread/read behavior matches expected flow.

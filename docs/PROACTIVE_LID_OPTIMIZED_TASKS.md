# Proactive LID Optimized Remediation Checklist

## Completed
- Extended `EnhancedMongoDBStoreConfig` to accept `connectionManager` overrides with safe defaults (900 global connection cap, tier sizing hooks).
- Reduced per-operation MongoDB pings by caching successful heartbeat timestamps and delegating health monitoring to `ConnectionHealthMonitor`.
- Normalized dedicated connection pooling defaults (20/4) to limit blast radius when shared pooling is unavailable.
- Ensured `ConnectionManager` honors `poolStrategy: 'dedicated'`, skips saturated pools, and enforces the global connection ceiling before allocating new pools.
- Wrapped `clearAll` in a best-effort MongoDB transaction with majority write concern and provided a deterministic fallback when transactions are unavailable.
- Wired the memory backpressure controller into `withConnection` so all datastore operations respect memory pressure and emit pause/resume telemetry.

## In Progress
- Add a lightweight stress harness that spins up 2,000 logical instances against a mocked driver to validate listener counts, pool utilisation, and backpressure behaviour.
- Document operational runbooks (connection pool sizing, monitoring alerts, and load test procedure) in `docs/operations/proactive-lid-optimized.md`.

## Next
- Once the harness is ready, integrate it into CI under a nightly job to guard against regressions.
- Capture production tuning guidance (Atlas connection dashboards, Redis saturation thresholds) in the public README.

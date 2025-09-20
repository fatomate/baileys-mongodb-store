# Proactive LID Optimized Operations Guide

_This document captures the operational practices that accompany the "proactive-lid-optimized" branch. It is a work in progress; fill in the outstanding sections as stress testing results arrive._

## Connection Pool Configuration
- Default global ceiling: **900** connections via `connectionManager.maxTotalConnections`.
- Tier sizing template:
  - hot: `maxPoolSize=60`, `minPoolSize=24`, `maxInstancesPerPool=25`, `maxIdleTimeMS=60000`.
  - warm: `maxPoolSize=24`, `minPoolSize=8`, `maxInstancesPerPool=60`, `maxIdleTimeMS=120000`.
  - cold: `maxPoolSize=10`, `minPoolSize=2`, `maxInstancesPerPool=150`, `maxIdleTimeMS=300000`.
- Dedicated fallback clients default to `maxPoolSize=20`, `minPoolSize=4`, `maxIdleTimeMS=30000`.

## Monitoring Checklist
- Track Atlas `connections.current` and alert at 80% of the configured ceiling.
- Watch Redis `used_memory` and `connected_clients` for queue congestion.
- Surface `connectionManager.getMetrics()` via Prometheus/Grafana for per-tier utilisation.
- Log watchers should look for `Memory pressure high, throttling new work` entries.

## Load Verification (TODO)
- [ ] Implement scripted stress harness that registers 2,000 instances via `getConnectionManager`.
- [ ] Capture baseline metrics (listener counts, heap snapshots) pre/post run.
- [ ] Automate via `npm run stress:connections` once harness exists.

## Incident Response
- If backpressure remains engaged for >5 minutes, scale application pods horizontally or raise connection limits gradually (+10%).
- On repeated transactional `clearAll` failures, fall back to manual cleanup script and open a MongoDB support ticket with the returned error signature.

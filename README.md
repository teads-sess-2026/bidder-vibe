# Bidder Template

Spring Boot bidder skeleton for Summer School 2026.

## What to implement

Three areas are stubbed — look for `// TODO` comments:

| File | What to implement |
|---|---|
| `BiddingService.java` | `bid()` — decide whether/how much to bid; `computeBidPrice()` — your pricing strategy |
| `AuctionNoticeConsumer.java` | `consume()` — handle win and loss Kafka notices, track budget |
| `StatsService.java` | `getStats()`, `getCreativeStats()`, `getTargetingStats()`, `getTimeseries()` — the dashboard API |

See `STATS_API.md` for the full stats API contract.

## Quick start

```bash
# 1. Set your team name in application.properties and config.env
# 2. Start infrastructure (Postgres, Kafka, Redis, Prometheus, Grafana)
make infra-up

# 3. Run the bidder
make run

# 4. Or run with a custom port/id
make run-team PORT=8081 BIDDER_ID=team-alpha
```

## Endpoints

| Endpoint | Description |
|---|---|
| `POST /api/bid` | Receive a bid request from the SSP |
| `GET /api/budget` | Remaining budget per creative |
| `GET /health` | Health check |
| `GET /api/stats` | Overall performance snapshot |
| `GET /api/stats/creatives` | Per-creative breakdown |
| `GET /api/stats/targeting` | Breakdown by geo / device / segment |
| `GET /api/stats/timeseries` | Time-bucketed trend data |
| `GET /actuator/prometheus` | Prometheus metrics |

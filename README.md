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

---

## Workshop setup

Two public URLs are needed:

| What | Tool | Used for |
|---|---|---|
| Bidder HTTP endpoint | **ngrok** | SSP sends `/bid` requests to your machine |
| Kafka notifications | **Tailscale** | Your bidder receives auction results from SSP's Kafka |

---

### Step 1 — Join the Tailscale network

All participants must join the same Tailscale network as the SSP machine.

```bash
# Install (one-time)
brew install tailscale
sudo tailscale up
# → opens browser for login — use the account shared by the workshop organiser
```

Once connected, your machine can reach the SSP's Kafka directly.

---

### Step 2 — Expose your bidder with ngrok

```bash
# Install (one-time)
brew install ngrok
ngrok config add-authtoken <your-token>   # sign up at ngrok.com

# Start tunnel (keep this running)
ngrok http 8080
# → https://xxxx.ngrok-free.app
```

> The free plan gives a new random URL on every restart — update `config.env` each time.

---

### Step 3 — Configure config.env

Edit `config.env`:

```env
BIDDER_ID=your-team-name
BIDDER_ENDPOINT_URL=https://xxxx.ngrok-free.app     # your ngrok URL from Step 2
KAFKA_BOOTSTRAP_SERVERS=100.104.247.47:9093          # SSP machine's Tailscale IP (given by organiser)
```

---

### Step 4 — Register with the SSP

Tell the workshop organiser your:
- `BIDDER_ID` (team name)
- `BIDDER_ENDPOINT_URL` (ngrok URL)

They will add you to `ssp/bidders.json` on the SSP machine.

---

### Step 5 — Start infrastructure and bidder

Infrastructure (Postgres/Kafka/Redis/Prometheus/Grafana) lives in the `ssp` repo now — the
organiser starts it there with `make run`. This repo's bidder container joins that stack's
`ss2026-net` Docker network, so it must already be running before you do this:

```bash
make run
```

---

## All commands

```bash
make run                                       Start the bidder container (requires ssp's `make run` already running)
make run-team PORT=9001 BIDDER_ID=team-alpha   Start with custom port/id
make run-docker                                Alias for 'make run'
make restart-bidder                            Reset this bidder's budget and restart the container
make down                                      Stop the bidder container
make build                                     Compile and package (skip tests)
make test                                      Run unit tests
make clean                                     Remove build artifacts
```

---

## Key endpoints

| Endpoint | Description |
|---|---|
| `POST /api/bid` | Receive a bid request from the SSP |
| `GET /api/budget` | Current remaining budget (polled by SSP every second) |
| `GET /health` | Health check |
| `GET /api/stats` | Overall performance snapshot |
| `GET /api/stats/creatives` | Per-creative breakdown |
| `GET /api/stats/targeting` | Breakdown by geo / device / segment |
| `GET /api/stats/timeseries` | Time-bucketed trend data |
| `GET /actuator/prometheus` | Prometheus metrics |

---

## Configuration reference

| Variable | Description |
|---|---|
| `BIDDER_ID` | Your team's unique ID — must match `id` in `ssp/bidders.json` |
| `BIDDER_ENDPOINT_URL` | Your ngrok public URL |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` if co-located with Docker, `<tailscale-ip>:9093` if remote |
| `TAILSCALE_IP` | This machine's Tailscale IP (only needed if running Docker here) |

Bidder strategy, budget, and competition timing are in `src/main/resources/application.properties`.

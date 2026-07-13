# Bidder Stats API

Read-only performance metrics for a single bidder. A frontend can poll these to
render creative/performance dashboards.

## Conventions

- **Base URL:** the bidder host, e.g. `http://localhost:8080` (each bidder runs on its own port).
- **Method:** all endpoints are `GET`. No request body. No authentication.
- **Response `Content-Type`:** `application/json`.
- **JSON casing:** `snake_case`.
- **Types used below:**
  - `integer` — JSON number, no decimals (`0`, `1240`).
  - `number` — JSON float. Rates are rounded to 4 decimals; prices/money to ~3–4 decimals.
  - `string` — text.
  - `datetime` — ISO-8601 **local** date-time string, **no timezone**, e.g. `"2026-07-08T11:15:03"`.
- **Empty data:** endpoints always return `200` with zeroed counts / empty arrays — never `404`.
- **Rates when denominator is 0:** returned as `0` (never `null`, never divide-by-zero).
- **Error shape** (only for invalid enum params):
  ```json
  { "error": "invalid sort: foo" }
  ```
  returned with HTTP `400`.

---

## 1. `GET /api/stats`

Overall performance snapshot for the bidder.

**Query params:** none.

**Response `200`:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `bidder_id` | string | no | This bidder's id. |
| `generated_at` | datetime | no | When the snapshot was computed. |
| `total_auctions` | integer | no | Bid requests received (every `POST /api/bid`). |
| `bids` | integer | no | Requests we answered with a price. |
| `no_bids` | integer | no | Requests we passed on (`total_auctions - bids`). |
| `bid_rate` | number | no | `bids / total_auctions` (0..1). |
| `wins` | integer | no | Auctions won (Kafka-confirmed). |
| `win_rate` | number | no | `wins / bids` (0..1). |
| `win_rate_per_auction` | number | no | `wins / total_auctions` (0..1). |
| `avg_bid_price` | number | no | Mean price we offered. `0` if no bids. |
| `avg_win_price` | number | no | Mean clearing price we paid. `0` if no wins. |
| `total_spend` | number | no | Sum of clearing prices of our wins. |
| `remaining_budget` | number | no | Live remaining budget (from Redis). |
| `budget` | number | no | Configured starting budget. |
| `latency_ms` | object | no | Our own processing time per request. See below. |
| `no_bid_reasons` | object | no | Breakdown of why we passed. See below. |
| `pacing` | object | no | Spend rate + projection. See below. |

**`latency_ms` object:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `avg` | number | no | Mean latency in ms. `0` if no samples. |
| `p50` | integer | no | Median latency (ms). |
| `p95` | integer | no | 95th percentile latency (ms). |
| `max` | integer | no | Max latency (ms). |
| `count` | integer | no | Number of latency samples. |

**`no_bid_reasons` object** (fixed keys, each an `integer` count):

| Key | Description |
|---|---|
| `budget_exhausted` | Passed because remaining budget was ≤ 0. |
| `no_eligible_creative` | Passed because the bidder has no creatives at all. |
| `targeting_miss` | Passed because creatives exist but none matched the request's targeting. |

**`pacing` object:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `spend_per_minute` | number | no | `total_spend / elapsed_minutes`. `0` if nothing spent. |
| `elapsed_minutes` | number | no | Minutes since the first recorded request. |
| `projected_minutes_to_exhaustion` | number | **yes** | `remaining_budget / spend_per_minute`. `null` when `spend_per_minute` is 0 or budget already exhausted. |
| `budget_utilization` | number | no | `total_spend / budget` (0..1). |

**Example:**
```json
{
  "bidder_id": "team-alpha",
  "generated_at": "2026-07-08T11:15:03",
  "total_auctions": 1240,
  "bids": 1005,
  "no_bids": 235,
  "bid_rate": 0.8105,
  "wins": 74,
  "win_rate": 0.0736,
  "win_rate_per_auction": 0.0597,
  "avg_bid_price": 1.231,
  "avg_win_price": 1.148,
  "total_spend": 84.95,
  "remaining_budget": 15.05,
  "budget": 100.0,
  "latency_ms": { "avg": 3.4, "p50": 2, "p95": 9, "max": 44, "count": 1240 },
  "no_bid_reasons": { "budget_exhausted": 120, "no_eligible_creative": 40, "targeting_miss": 75 },
  "pacing": { "spend_per_minute": 4.2, "elapsed_minutes": 20.1, "projected_minutes_to_exhaustion": 3.58, "budget_utilization": 0.85 }
}
```

---

## 2. `GET /api/stats/creatives`

Per-creative breakdown.

**Query params:**

| Param | Type | Required | Default | Allowed values |
|---|---|---|---|---|
| `creative_id` | string | no | _(all)_ | Any creative id. When provided, only that creative is returned (empty array if it has no activity). |
| `sort` | string (enum) | no | `spend` | `spend`, `wins`, `bids`, `bid_rate`, `win_rate` |
| `order` | string (enum) | no | `desc` | `asc`, `desc` |

**Response `200`:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `bidder_id` | string | no | This bidder's id. |
| `creatives` | array\<Creative\> | no | One entry per creative that has any activity (filtered by `creative_id` if given). Sorted per `sort`/`order`. |

**`Creative` object:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `creative_id` | string | no | Creative id. |
| `creative_name` | string | **yes** | Display name. `null` if the creative is no longer known. |
| `bids` | integer | no | Bids submitted using this creative. |
| `wins` | integer | no | Wins attributed to this creative. |
| `win_rate` | number | no | `wins / bids` (0..1). |
| `avg_bid_price` | number | no | Mean offered price for this creative. |
| `avg_win_price` | number | no | Mean clearing price paid. `0` if no wins. |
| `spend` | number | no | Sum of clearing prices for this creative's wins. |

**Example:** `GET /api/stats/creatives?creative_id=team-alpha-creative-1`
```json
{
  "bidder_id": "team-alpha",
  "creatives": [
    {
      "creative_id": "team-alpha-creative-1",
      "creative_name": "Universal",
      "bids": 420,
      "wins": 31,
      "win_rate": 0.0738,
      "avg_bid_price": 1.22,
      "avg_win_price": 1.14,
      "spend": 35.3
    }
  ]
}
```

---

## 3. `GET /api/stats/targeting`

Breakdown by targeting dimension (where the bidder wins/loses).

**Query params:**

| Param | Type | Required | Default | Allowed values |
|---|---|---|---|---|
| `dimension` | string (enum) | no | `all` | `geo`, `device`, `segment`, `all` |

The response always contains all three keys (`by_geo`, `by_device`, `by_segment`) for a
stable shape. When `dimension` is a single value, the non-selected arrays are returned empty (`[]`).

**Response `200`:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `bidder_id` | string | no | This bidder's id. |
| `by_geo` | array\<TargetingBucket\> | no | Grouped by geo. `[]` if not requested. |
| `by_device` | array\<TargetingBucket\> | no | Grouped by device type. `[]` if not requested. |
| `by_segment` | array\<TargetingBucket\> | no | Grouped by audience segment. `[]` if not requested. |

**`TargetingBucket` object:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `key` | string | no | The geo / device / segment value (e.g. `"US"`, `"mobile"`, `"tech"`). |
| `bids` | integer | no | Bids submitted for this key. |
| `wins` | integer | no | Wins for this key. |
| `win_rate` | number | no | `wins / bids` (0..1). |
| `avg_bid_price` | number | no | Mean offered price for this key. |

**Example:**
```json
{
  "bidder_id": "team-alpha",
  "by_geo":     [ { "key": "US",     "bids": 120, "wins": 12, "win_rate": 0.10,  "avg_bid_price": 1.20 } ],
  "by_device":  [ { "key": "mobile", "bids": 400, "wins": 30, "win_rate": 0.075, "avg_bid_price": 1.25 } ],
  "by_segment": [ { "key": "tech",   "bids": 210, "wins": 18, "win_rate": 0.086, "avg_bid_price": 1.30 } ]
}
```

---

## 4. `GET /api/stats/timeseries`

Per-bucket trend over a recent window, for charting.

**Query params:**

| Param | Type | Required | Default | Range / notes |
|---|---|---|---|---|
| `window_minutes` | integer | no | `30` | `1`–`180`; values outside are **clamped** (not an error). |
| `bucket_seconds` | integer | no | `60` | min `10`; values below are **clamped**. |

**Response `200`:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `bidder_id` | string | no | This bidder's id. |
| `window_minutes` | integer | no | Effective window (after clamping). |
| `bucket_seconds` | integer | no | Effective bucket size (after clamping). |
| `points` | array\<Point\> | no | Chronological, oldest first. **Every bucket in the window is present** (empty ones are zero-filled) so the series is continuous. Length is always `floor(window_minutes × 60 / bucket_seconds)` — e.g. **30 points** for the defaults (30 min / 60s). |

**`Point` object:**

| Field | Type | Nullable | Description |
|---|---|---|---|
| `time` | datetime | no | Bucket start time. |
| `auctions` | integer | no | Requests received in this bucket. |
| `bids` | integer | no | Bids submitted in this bucket. |
| `wins` | integer | no | Wins in this bucket. |
| `bid_rate` | number | no | `bids / auctions` (0..1). |
| `win_rate` | number | no | `wins / bids` (0..1). |
| `avg_bid_price` | number | no | Mean offered price in this bucket. `0` if no bids. |
| `spend` | number | no | Clearing-price spend in this bucket. |

**Example** (defaults → 30 one-minute buckets; array below truncated with `…` — a real
response always contains all 30 points, including zero-filled idle minutes):
```json
{
  "bidder_id": "team-alpha",
  "window_minutes": 30,
  "bucket_seconds": 60,
  "points": [
    { "time": "2026-07-08T10:46:00", "auctions": 36, "bids": 30, "wins": 2, "bid_rate": 0.833, "win_rate": 0.0667, "avg_bid_price": 1.24, "spend": 2.30 },
    { "time": "2026-07-08T10:47:00", "auctions": 0,  "bids": 0,  "wins": 0, "bid_rate": 0.0,   "win_rate": 0.0,    "avg_bid_price": 0.0,  "spend": 0.0 },
    { "time": "2026-07-08T10:48:00", "auctions": 34, "bids": 28, "wins": 1, "bid_rate": 0.824, "win_rate": 0.0357, "avg_bid_price": 1.19, "spend": 1.12 }
  ]
}
```
> The `…` is illustrative only — JSON has no ellipsis. The actual `points` array here would have exactly 30 entries.

---

## Enum reference (quick)

| Enum | Values |
|---|---|
| `creatives.sort` | `spend`, `wins`, `bids`, `bid_rate`, `win_rate` |
| `creatives.order` | `asc`, `desc` |
| `targeting.dimension` | `geo`, `device`, `segment`, `all` |
| `no_bid_reasons` keys | `budget_exhausted`, `no_eligible_creative`, `targeting_miss` |

## Notes for the frontend

- All numbers are **durable** (persisted in the bidder's Postgres schema `bid_record` + `win_notice`), except `remaining_budget`/`budget` which come from the live Redis budget cache. Values survive a bidder restart.
- `total_auctions`, `bids`, `no_bids`, `latency_ms`, `no_bid_reasons` are **all-time** cumulative counts.
- `timeseries` is the only windowed endpoint; use it for charts, use `/api/stats` for headline KPIs.
- Safe to poll `/api/stats` every few seconds; `timeseries` every 5–15s is plenty.

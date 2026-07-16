package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis, plus durable market-learning stats.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * The SSP is the SINGLE OWNER of spend on these keys: it atomically decrements them
 * on each win. This bidder never writes spend — it only seeds a key once with
 * setIfAbsent (so a restart can't refill an already-spent budget) and reads the
 * remaining value to decide whether a creative can still spend.
 *
 * <p>The market-learning signals that drive computeBidPrice — the recent clearing-price
 * window and the win count — are mirrored to Redis ({@code {bidderId}_win_prices} list,
 * {@code {bidderId}_win_count} counter) so they survive a restart and are shared across
 * replicas. The in-memory copies stay the fast read path for the reactive bid loop; Redis
 * is the durable source of truth, re-hydrated into memory once on startup.
 */
@Component
public class BidderStatsCache {

    private static final Logger log = LoggerFactory.getLogger(BidderStatsCache.class);

    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;

    private final AtomicLong winCount = new AtomicLong(0);
    private final Deque<Double> recentWinPrices = new ArrayDeque<>();

    // The observed *market* price window: clearing prices from auctions we won AND lost.
    // Loss clearing prices are the market signal the bidder was previously blind to — they
    // tell us what it cost to win auctions we didn't, so pricing off this fuller window
    // (rather than win prices alone, which only ever sample at/below what we already win)
    // lets us bid just enough to clear the market instead of overshooting.
    private final Deque<Double> recentMarketPrices = new ArrayDeque<>();

    public BidderStatsCache(BidderProperties properties, ReactiveRedisTemplate<String, String> redis) {
        this.properties = properties;
        this.redis = redis;
    }

    /** Redis key holding the remaining budget for one creative. */
    public String budgetKey(String creativeId) {
        return properties.getId() + "_" + creativeId + "_budget";
    }

    /** Redis key holding the capped list of recent clearing prices for this bidder. */
    private String winPricesKey() {
        return properties.getId() + "_win_prices";
    }

    /** Redis key holding this bidder's cumulative win count. */
    private String winCountKey() {
        return properties.getId() + "_win_count";
    }

    /** Redis key holding the capped list of recent market clearing prices (wins + losses). */
    private String marketPricesKey() {
        return properties.getId() + "_market_prices";
    }

    /** Redis key holding the pacing anchor — the instant this bidder first started spending. */
    private String pacingAnchorKey() {
        return properties.getId() + "_pacing_anchor";
    }

    /**
     * Re-hydrate the in-memory learning signals from Redis on startup so a restarted (or
     * freshly-scaled) bidder resumes with the market it already learned instead of falling
     * back to cold-start pricing. Best-effort: a Redis miss/error just leaves the mirror empty.
     */
    @PostConstruct
    void warmLoadStats() {
        try {
            Long count = redis.opsForValue().get(winCountKey())
                    .map(Long::parseLong)
                    .onErrorReturn(0L)
                    .block();
            if (count != null) {
                winCount.set(count);
            }
            List<String> prices = redis.opsForList().range(winPricesKey(), 0, -1)
                    .collectList()
                    .onErrorReturn(List.of())
                    .block();
            if (prices != null && !prices.isEmpty()) {
                synchronized (recentWinPrices) {
                    recentWinPrices.clear();
                    for (String p : prices) {
                        try {
                            recentWinPrices.addLast(Double.parseDouble(p));
                        } catch (NumberFormatException ignored) {
                            // Skip a malformed entry rather than abort the whole warm-load.
                        }
                    }
                }
            }
            List<String> marketPrices = redis.opsForList().range(marketPricesKey(), 0, -1)
                    .collectList()
                    .onErrorReturn(List.of())
                    .block();
            if (marketPrices != null && !marketPrices.isEmpty()) {
                synchronized (recentMarketPrices) {
                    recentMarketPrices.clear();
                    for (String p : marketPrices) {
                        try {
                            recentMarketPrices.addLast(Double.parseDouble(p));
                        } catch (NumberFormatException ignored) {
                            // Skip a malformed entry rather than abort the whole warm-load.
                        }
                    }
                }
            }
            log.info("Warm-loaded market stats from Redis: winCount={} winPriceSamples={} marketPriceSamples={}",
                    winCount.get(), recentWinPrices.size(), recentMarketPrices.size());
        } catch (Exception e) {
            log.warn("Market-stats warm-load skipped (non-fatal): {}", e.getMessage());
        }
    }

    /**
     * Seed a creative's remaining budget to its full limit, only if the key doesn't exist yet
     * (SETNX semantics). The SSP owns spend on this key, so an unconditional SET on a bidder
     * restart would refill an already-spent budget. Called once per creative on startup.
     */
    public Mono<Boolean> initBudget(String creativeId, double budget) {
        String key = budgetKey(creativeId);
        return redis.opsForValue().setIfAbsent(key, String.valueOf(budget))
                .doOnNext(seeded -> {
                    if (seeded) {
                        log.info("Creative budget seeded: {} = {}", key, budget);
                    } else {
                        log.info("Creative budget already exists — left untouched: {}", key);
                    }
                });
    }

    /**
     * Record a Kafka-confirmed win in the local and durable market-learning stats. The budget
     * key itself is NOT touched here — the SSP is the single owner of budget spend and has
     * already decremented {@code {bidderId}_{creativeId}_budget} atomically on the win.
     */
    public Mono<Void> recordWin(String creativeId, double clearingPrice) {
        // Fast in-memory read path for the reactive bid loop.
        winCount.incrementAndGet();
        pushCapped(recentWinPrices, clearingPrice);
        // A win's clearing price is also a market observation.
        pushCapped(recentMarketPrices, clearingPrice);
        // Durable, cross-restart/replica copy of the same signals. Best-effort: errors are
        // swallowed inside recordWinStats — the in-memory mirror above still reflects this win.
        return recordWinStats(clearingPrice);
    }

    /** Persist the win to the durable Redis learning signals (win count + capped price windows). */
    private Mono<Void> recordWinStats(double clearingPrice) {
        int windowSize = properties.getStrategy().getWindowSize();
        return redis.opsForValue().increment(winCountKey())
                .then(redis.opsForList().rightPush(winPricesKey(), String.valueOf(clearingPrice)))
                // Keep only the last windowSize entries — mirrors the in-memory deque cap.
                .then(redis.opsForList().trim(winPricesKey(), -windowSize, -1))
                // A win's clearing price is also a market observation.
                .then(pushMarketPriceRedis(clearingPrice))
                .onErrorResume(e -> {
                    log.warn("Durable win-stats update failed (non-fatal): {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    /**
     * Record the clearing price of an auction we bid on but LOST. This is the market signal
     * the bidder was previously blind to: it's exactly what it would have cost to win, so it
     * anchors pricing to the real market rather than to the (cheaper) subset we already win.
     * Updates the fast in-memory market window immediately, then mirrors to Redis best-effort.
     * Safe to call from the Kafka consumer thread.
     */
    public Mono<Void> recordLoss(double clearingPrice) {
        pushCapped(recentMarketPrices, clearingPrice);
        return pushMarketPriceRedis(clearingPrice)
                .onErrorResume(e -> {
                    log.warn("Durable loss-price update failed (non-fatal): {}", e.getMessage());
                    return Mono.empty();
                });
    }

    /** Append a market clearing price to the durable Redis window, trimmed to windowSize. */
    private Mono<Void> pushMarketPriceRedis(double clearingPrice) {
        int windowSize = properties.getStrategy().getWindowSize();
        return redis.opsForList().rightPush(marketPricesKey(), String.valueOf(clearingPrice))
                .then(redis.opsForList().trim(marketPricesKey(), -windowSize, -1))
                .then();
    }

    /** Append to a capped in-memory price window under its own lock. */
    private void pushCapped(Deque<Double> window, double price) {
        synchronized (window) {
            window.addLast(price);
            if (window.size() > properties.getStrategy().getWindowSize()) {
                window.pollFirst();
            }
        }
    }

    /** Remaining budget for a creative. Lazily initializes to the flat creative budget if missing. */
    public Mono<Double> getRemainingBudget(String creativeId) {
        String key = budgetKey(creativeId);
        double defaultBudget = properties.getCreativeBudget();
        return redis.opsForValue().get(key)
                .flatMap(val -> {
                    try {
                        return Mono.just(Double.parseDouble(val));
                    } catch (NumberFormatException e) {
                        return Mono.just(defaultBudget);
                    }
                })
                .switchIfEmpty(redis.opsForValue().setIfAbsent(key, String.valueOf(defaultBudget))
                        .thenReturn(defaultBudget))
                // This read is on the bid hot path. A Redis timeout/error here would otherwise
                // propagate up and blow the request's 300ms deadline (a logged BID TIMEOUT = a lost
                // auction). Treat a transient failure as "budget available" so we still bid; the
                // SSP-owned budget key remains the source of truth for actual spend.
                .onErrorResume(e -> {
                    log.warn("Budget read failed for {} — assuming full budget for this bid (non-fatal): {}",
                            key, e.getMessage());
                    return Mono.just(defaultBudget);
                });
    }

    /**
     * The pacing anchor: the instant this bidder first started spending, used by pacing to
     * compute how far into the competition window we are. Persisted in Redis with setIfAbsent
     * so the FIRST boot's instant wins and survives restarts/replicas — a restarted bidder keeps
     * pacing off its original start instead of re-anchoring to the restart moment (which would
     * make it think it's early again and under-spend the rest of the window). Best-effort: on any
     * Redis error, fall back to the caller-supplied instant (this JVM's boot time).
     */
    public Mono<Instant> getOrInitPacingAnchor(Instant fallback) {
        String key = pacingAnchorKey();
        return redis.opsForValue().setIfAbsent(key, fallback.toString())
                .then(redis.opsForValue().get(key))
                .map(Instant::parse)
                .onErrorResume(e -> {
                    log.warn("Pacing-anchor init failed, using boot instant {} (non-fatal): {}",
                            fallback, e.getMessage());
                    return Mono.just(fallback);
                })
                .defaultIfEmpty(fallback);
    }

    public long getWinCount() {
        return winCount.get();
    }

    public double getRollingAverageWinPrice() {
        return averageOf(recentWinPrices);
    }

    /**
     * Rolling average of recent market clearing prices (wins AND losses) — the fuller,
     * less-biased estimate of what it currently costs to win. Prefer this over
     * getRollingAverageWinPrice() for pricing once loss samples exist.
     */
    public double getRollingAverageMarketPrice() {
        return averageOf(recentMarketPrices);
    }

    /** Number of clearing-price samples (wins + losses) in the market window. */
    public long getMarketSampleCount() {
        synchronized (recentMarketPrices) {
            return recentMarketPrices.size();
        }
    }

    private double averageOf(Deque<Double> window) {
        synchronized (window) {
            if (window.isEmpty()) return 0.0;
            return window.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
    }

    public long getSampleCount() {
        return winCount.get();
    }
}

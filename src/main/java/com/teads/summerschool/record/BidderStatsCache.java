package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeRepository;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis, plus durable market-learning stats.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * Each creative has its own budget limit; remaining decreases on each Kafka-confirmed
 * win for that creative. Both this bidder and the SSP read these keys to decide whether
 * a creative can still spend. Postgres's {@code creatives.budget} column is kept in sync
 * with the same remaining value so it isn't lost if Redis is wiped.
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

    // KEYS[1] = budget key, ARGV[1] = default budget (used only if the key doesn't exist yet),
    // ARGV[2] = clearing price to subtract. Atomic on the Redis server itself, replacing the old
    // synchronized setIfAbsent()-then-increment() pair, which only ever guarded against
    // concurrent callers within this one JVM, not against Redis itself.
    private static final RedisScript<Double> RECORD_WIN_SCRIPT = RedisScript.of("""
            if redis.call('EXISTS', KEYS[1]) == 0 then
                redis.call('SET', KEYS[1], ARGV[1])
            end
            return redis.call('INCRBYFLOAT', KEYS[1], -tonumber(ARGV[2]))
            """, Double.class);

    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;
    private final CreativeRepository creativeRepository;

    private final AtomicLong winCount = new AtomicLong(0);
    private final Deque<Double> recentWinPrices = new ArrayDeque<>();

    public BidderStatsCache(BidderProperties properties, ReactiveRedisTemplate<String, String> redis,
                             CreativeRepository creativeRepository) {
        this.properties = properties;
        this.redis = redis;
        this.creativeRepository = creativeRepository;
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
            log.info("Warm-loaded market stats from Redis: winCount={} priceSamples={}",
                    winCount.get(), recentWinPrices.size());
        } catch (Exception e) {
            log.warn("Market-stats warm-load skipped (non-fatal): {}", e.getMessage());
        }
    }

    /** Set a creative's remaining budget to its full limit. Called once per creative on startup. */
    public Mono<Boolean> initBudget(String creativeId, double budget) {
        String key = budgetKey(creativeId);
        return redis.opsForValue().set(key, String.valueOf(budget))
                .doOnNext(ok -> log.info("Creative budget initialized: {} = {}", key, budget));
    }

    /** Decrement the winning creative's remaining budget by what it paid. */
    public Mono<Double> recordWin(String creativeId, double clearingPrice) {
        String key = budgetKey(creativeId);
        return redis.execute(RECORD_WIN_SCRIPT,
                        List.of(key),
                        List.of(String.valueOf(properties.getCreativeBudget()), String.valueOf(clearingPrice)))
                .next()
                .doOnNext(after -> log.info("BUDGET  key={} clearing={} remaining={}", key, clearingPrice, after))
                .flatMap(after -> creativeRepository.findById(creativeId)
                        .flatMap(c -> {
                            c.setBudget(after);
                            return creativeRepository.save(c);
                        })
                        .thenReturn(after))
                .doOnNext(after -> {
                    // Fast in-memory read path for the reactive bid loop.
                    winCount.incrementAndGet();
                    synchronized (recentWinPrices) {
                        recentWinPrices.addLast(clearingPrice);
                        if (recentWinPrices.size() > properties.getStrategy().getWindowSize()) {
                            recentWinPrices.pollFirst();
                        }
                    }
                })
                // Durable, cross-restart/replica copy of the same signals. Best-effort: a Redis
                // failure here must not undo the (already-committed) budget decrement, so swallow
                // errors — the in-memory mirror above still reflects this win.
                .flatMap(after -> recordWinStats(clearingPrice).thenReturn(after));
    }

    /** Persist the win to the durable Redis learning signals (win count + capped price window). */
    private Mono<Void> recordWinStats(double clearingPrice) {
        int windowSize = properties.getStrategy().getWindowSize();
        return redis.opsForValue().increment(winCountKey())
                .then(redis.opsForList().rightPush(winPricesKey(), String.valueOf(clearingPrice)))
                // Keep only the last windowSize entries — mirrors the in-memory deque cap.
                .then(redis.opsForList().trim(winPricesKey(), -windowSize, -1))
                .onErrorResume(e -> {
                    log.warn("Durable win-stats update failed (non-fatal): {}", e.getMessage());
                    return Mono.empty();
                })
                .then();
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
                        .thenReturn(defaultBudget));
    }

    public long getWinCount() {
        return winCount.get();
    }

    public double getRollingAverageWinPrice() {
        synchronized (recentWinPrices) {
            if (recentWinPrices.isEmpty()) return 0.0;
            return recentWinPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
        }
    }

    public long getSampleCount() {
        return winCount.get();
    }
}

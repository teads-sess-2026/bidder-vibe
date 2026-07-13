package com.teads.summerschool.record;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.CreativeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-creative budget cache backed by Redis.
 *
 * <p>Key format: {@code {bidderId}_{creativeId}_budget}, value = remaining budget.
 * Each creative has its own budget limit; remaining decreases on each Kafka-confirmed
 * win for that creative. Both this bidder and the SSP read these keys to decide whether
 * a creative can still spend. Postgres's {@code creatives.budget} column is kept in sync
 * with the same remaining value so it isn't lost if Redis is wiped.
 */
@Component
public class BidderStatsCache {

    private static final Logger log = LoggerFactory.getLogger(BidderStatsCache.class);

    private final BidderProperties properties;
    private final StringRedisTemplate redis;
    private final CreativeRepository creativeRepository;

    private final AtomicLong winCount = new AtomicLong(0);
    private final Deque<Double> recentWinPrices = new ArrayDeque<>();

    public BidderStatsCache(BidderProperties properties, StringRedisTemplate redis, CreativeRepository creativeRepository) {
        this.properties = properties;
        this.redis = redis;
        this.creativeRepository = creativeRepository;
    }

    /** Redis key holding the remaining budget for one creative. */
    public String budgetKey(String creativeId) {
        return properties.getId() + "_" + creativeId + "_budget";
    }

    /** Set a creative's remaining budget to its full limit. Called once per creative on startup. */
    public void initBudget(String creativeId, double budget) {
        String key = budgetKey(creativeId);
        redis.opsForValue().set(key, String.valueOf(budget));
        log.info("Creative budget initialized: {} = {}", key, budget);
    }

    /** Decrement the winning creative's remaining budget by what it paid. */
    public synchronized void recordWin(String creativeId, double clearingPrice) {
        String key = budgetKey(creativeId);
        redis.opsForValue().setIfAbsent(key, String.valueOf(properties.getCreativeBudget()));
        Double after = redis.opsForValue().increment(key, -clearingPrice);
        log.info("BUDGET  key={} clearing={} remaining={}", key, clearingPrice, after);

        creativeRepository.findById(creativeId).ifPresent(c -> {
            c.setBudget(after);
            creativeRepository.save(c);
        });

        winCount.incrementAndGet();
        recentWinPrices.addLast(clearingPrice);
        if (recentWinPrices.size() > properties.getStrategy().getWindowSize()) {
            recentWinPrices.pollFirst();
        }
    }

    /** Remaining budget for a creative. Lazily initializes to the flat creative budget if missing. */
    public double getRemainingBudget(String creativeId) {
        String key = budgetKey(creativeId);
        String val = redis.opsForValue().get(key);
        if (val == null) {
            redis.opsForValue().setIfAbsent(key, String.valueOf(properties.getCreativeBudget()));
            return properties.getCreativeBudget();
        }
        try {
            return Double.parseDouble(val);
        } catch (NumberFormatException e) {
            return properties.getCreativeBudget();
        }
    }

    public long getWinCount() {
        return winCount.get();
    }

    public synchronized double getRollingAverageWinPrice() {
        if (recentWinPrices.isEmpty()) return 0.0;
        return recentWinPrices.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }

    public long getSampleCount() {
        return winCount.get();
    }
}

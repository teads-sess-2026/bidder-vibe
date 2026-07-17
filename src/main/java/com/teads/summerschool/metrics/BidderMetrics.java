package com.teads.summerschool.metrics;

import com.teads.summerschool.config.BidderProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.util.function.Supplier;
import java.util.concurrent.TimeUnit;

/**
 * Prometheus/Micrometer metrics for the bidder, exposed at /actuator/prometheus.
 * Every meter is tagged with the bidder id so multiple bidders can be told apart.
 */
@Component
public class BidderMetrics {

    private final MeterRegistry registry;
    private final Counter requests;
    private final Counter bids;
    private final Counter wins;
    private final Counter losses;
    private final Counter spend;
    private final Timer bidLatency;
    // Price distributions (avg + p50/p95) so we can watch that bids stay a small multiple of the
    // floor and never creep back toward the $25 budget cap, and see what wins actually clear at.
    private final DistributionSummary bidPrice;
    private final DistributionSummary winClearingPrice;

    private final String prefix;

    public BidderMetrics(MeterRegistry registry, BidderProperties properties) {
        this.registry = registry;
        // Every metric name is prefixed with the group (bidder id), e.g.
        // team-alpha -> "team_alpha.bids" -> Prometheus "team_alpha_bids_total".
        this.prefix = properties.getId().toLowerCase().replaceAll("[^a-z0-9]+", "_") + ".";
        registry.config().commonTags("bidder", properties.getId());
        // Note: no ".total" suffix — Micrometer appends "_total" to counters for Prometheus.
        this.requests = Counter.builder(prefix + "requests")
                .description("Bid requests received").register(registry);
        this.bids = Counter.builder(prefix + "bids")
                .description("Bids submitted").register(registry);
        this.wins = Counter.builder(prefix + "wins")
                .description("Auctions won").register(registry);
        this.losses = Counter.builder(prefix + "losses")
                .description("Auctions lost after bidding").register(registry);
        this.spend = Counter.builder(prefix + "spend")
                .description("Total clearing price paid").register(registry);
        this.bidLatency = Timer.builder(prefix + "bid.latency")
                .description("Bid handling latency").register(registry);
        this.bidPrice = DistributionSummary.builder(prefix + "bid.price")
                .description("Submitted bid price").baseUnit("usd")
                .publishPercentiles(0.5, 0.95).register(registry);
        this.winClearingPrice = DistributionSummary.builder(prefix + "win.clearing.price")
                .description("Clearing price actually paid on wins").baseUnit("usd")
                .publishPercentiles(0.5, 0.95).register(registry);
    }

    public void recordRequest() { requests.increment(); }

    public void recordBid() { bids.increment(); }

    public void recordNoBid(String reason) {
        registry.counter(prefix + "nobids", "reason", reason == null ? "unknown" : reason).increment();
    }

    public void recordLatency(long ms) { bidLatency.record(ms, TimeUnit.MILLISECONDS); }

    public void recordWin(double clearingPrice) {
        wins.increment();
        spend.increment(clearingPrice);
    }

    public void recordLoss() { losses.increment(); }

    /** Record the price we bid (populates the bid-price distribution). */
    public void recordBidPrice(double price) { bidPrice.record(price); }

    /** Record the clearing price actually paid on a confirmed win. */
    public void recordWinClearingPrice(double clearingPrice) { winClearingPrice.record(clearingPrice); }

    /** Register a live gauge (e.g. remaining budget); the group prefix is applied. */
    public void registerGauge(String name, Supplier<Number> supplier) {
        Gauge.builder(prefix + name, supplier).register(registry);
    }

    
}

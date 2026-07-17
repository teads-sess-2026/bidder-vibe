package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);

    private final BidderProperties properties;
    private final CreativeCache creativeCache;
    private final BidRecordRepository bidRecordRepository;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    private volatile double lastKnownBudget = 0.0;


    private final Instant bootInstant = Instant.now();
    private volatile Instant pacingAnchor = bootInstant;


    private volatile Map<String, Double> budgetSnapshot = new java.util.concurrent.ConcurrentHashMap<>();
    private volatile long lastBudgetFetch = 0;


    private final java.util.concurrent.atomic.AtomicLong totalSpentCents = new java.util.concurrent.atomic.AtomicLong(0);


    private static final double PACING_TOLERANCE = 1.50;

    // Persist ~1 in RECORD_SAMPLE_RATE bid records to Postgres (see saveRecordAsync) — cuts DB
    // write load ~10x while keeping dashboard aggregates representative.
    private static final int RECORD_SAMPLE_RATE = 10;
    private final java.util.concurrent.atomic.AtomicLong recordSampleCounter = new java.util.concurrent.atomic.AtomicLong(0);

    public BiddingService(BidderProperties properties,
                          CreativeCache creativeCache,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics,
                          OwnBidCache ownBidCache) {
        this.properties = properties;
        this.creativeCache = creativeCache;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudgetSafe);

        try {
            Instant resolved = statsCache.getOrInitPacingAnchor(bootInstant).block();
            if (resolved != null) {
                pacingAnchor = resolved;
            }
        } catch (Exception e) {
            log.warn("Pacing-anchor resolve failed, using boot instant {} (non-fatal): {}",
                    bootInstant, e.getMessage());
        }
        String configured = properties.getCompetition().getStartTime();
        boolean hasConfigured = configured != null && !configured.isBlank();
        log.info("Pacing anchor: {} (duration={}s)",
                hasConfigured ? "configured=" + configured : "persisted-boot=" + pacingAnchor,
                properties.getCompetition().getDurationSeconds());
    }

    private double getRemainingBudgetSafe() {
        try {
            Double value = getRemainingBudget()
                    .timeout(Duration.ofMillis(properties.getTimeoutMs()))
                    .onErrorReturn(lastKnownBudget)
                    .block();
            lastKnownBudget = value;
            return value;
        } catch (Exception ex) {
            return lastKnownBudget;
        }
    }

    public Mono<Optional<BidResponse>> bid(BidRequest request) {
        long start = System.nanoTime();
        metrics.recordRequest();
        BidRecord record = buildRecord(request);
        double floor = request.floorPrice();

        // A null targeting block is treated as fully unrestricted (matches wildcard creatives).
        String geo = request.targeting() == null ? null : request.targeting().geo();
        String deviceType = request.targeting() == null ? null : request.targeting().deviceType();
        String audienceSegment = request.targeting() == null ? null : request.targeting().audienceSegment();

        // Read the catalog from the in-memory snapshot — no Redis GET / JSON deserialize per bid.
        List<Creative> all = creativeCache.getAllCached();
        if (all.isEmpty()) {
            return noBid(record, "no_eligible_creative", start);
        }
            // F3: a creative must never be chosen for a floor above its cap — checked before
            // targeting and budget.
            List<Creative> withinMax = all.stream()
                    .filter(c -> c.isWithinMaxBid(floor))
                    .toList();
            if (withinMax.isEmpty()) {
                return noBid(record, "floor_exceeds_max_bid", start);
            }
            // F1: targeting match.
            List<Creative> matched = withinMax.stream()
                    .filter(c -> c.matches(geo, deviceType, audienceSegment))
                    .toList();
            if (matched.isEmpty()) {
                return noBid(record, "targeting_miss", start);
            }
            // F2: only creatives that still have budget. Read the in-memory budget snapshot
            // (refreshed ~1s off the event loop) instead of one Redis GET per matched creative on
            // the hot path — a synchronous O(1) lookup. A missing key defaults to the full budget
            // so a cold/empty snapshot never wrongly no-bids; a small floor (> 5) skips creatives
            // that are effectively drained.
            Map<String, Double> budgets = getBudgetSnapshot();
            double full = properties.getCreativeBudget();
            List<Map.Entry<Creative, Double>> eligible = matched.stream()
                    .map(c -> Map.entry(c, budgets.getOrDefault(c.getId(), full)))
                    .filter(e -> e.getValue() > 5.0)
                    .toList();
            if (eligible.isEmpty()) {
                return noBid(record, "budget_exhausted", start);
            }
            // Prefer the most specific creative (fewer wildcard fields), so the
            // Universal creative is a fallback rather than the default; tie-break on
            // higher remaining budget to spread spend across creatives.
            Map.Entry<Creative, Double> best = eligible.stream()
                    .max(Comparator
                            .comparingInt((Map.Entry<Creative, Double> e) -> specificity(e.getKey()))
                            .thenComparingDouble(Map.Entry::getValue))
                    .orElseThrow();
            Creative creative = best.getKey();
            double remaining = best.getValue();

            // Linear pacing: skip only when we are running ahead of the EVEN target spend line for
            // this point in the window (see shouldBid). Unlike the old back-loaded throttle, this
            // fills steadily across the whole competition rather than banking budget for the endgame.
            if (!shouldBid()) {
                return noBid(record, "paced", start);
            }

            PacingState pacing = pacingState(remaining);
            double bidPrice = computeBidPrice(request, pacing);

            // Let the win-notice consumer attribute a win to this bid without a DB hit.
            ownBidCache.record(request.requestId(), creative.getId(), bidPrice);

            record.setBidPrice(bidPrice);
            record.setCreativeId(creative.getId());
            record.setLatencyMs(elapsedMs(start));
            metrics.recordBid();
            metrics.recordLatency(elapsedMs(start));

            BidResponse response = new BidResponse(
                    request.requestId(), bidPrice, toCreativeDto(creative));
            // Persist the bid record OFF the response path: the SSP response must not wait
            // on a Postgres INSERT (a shared DB under load was our biggest latency/timeout
            // source). Fire-and-forget on the DB scheduler; a write failure is logged, not
            // surfaced to the auction.
            saveRecordAsync(record);
            return Mono.just(Optional.of(response));
    }

    /** Record a no-bid with its reason + latency and return an empty (204) response. */
    private Mono<Optional<BidResponse>> noBid(BidRecord record, String reason, long start) {
        record.setNoBidReason(reason);
        record.setLatencyMs(elapsedMs(start));
        metrics.recordNoBid(reason);
        metrics.recordLatency(elapsedMs(start));
        // As with a bid, keep the Postgres write off the response path — even 204s were paying a full
        // INSERT round trip before responding.
        saveRecordAsync(record);
        return Mono.just(Optional.empty());
    }

    /**
     * Persist a SAMPLE of bid records without blocking the auction response. Only ~1 in
     * {@link #RECORD_SAMPLE_RATE} records is written, cutting Postgres INSERT volume ~10x while
     * keeping StatsService aggregates (latency percentiles, no-bid reason mix, per-dimension win
     * rates) representative. Absolute dashboard counts are therefore a ~10x-scaled sample; exact
     * win counts still come from WinNotice/Kafka, not from these records. The sampled write is
     * subscribed on the bounded-elastic scheduler so the blocking-capable R2DBC save never runs on
     * the Netty event loop, and any error is logged rather than failing the bid we already returned.
     */
    private void saveRecordAsync(BidRecord record) {
        if (recordSampleCounter.incrementAndGet() % RECORD_SAMPLE_RATE != 0) {
            return;
        }
        bidRecordRepository.save(record)
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                        saved -> {},
                        e -> log.warn("Async bid-record save failed (non-fatal): {}", e.getMessage()));
    }

    /**
     * Efficiency-first price: estimate the market clearing price and bid just enough over it.
     * Cold start (too few observed wins) falls back to a small markup over the floor. A pacing
     * multiplier then scales the bid up or down to keep the creative's budget alive across the
     * whole competition window — this is the sole price modifier. Always kept strictly above the
     * floor and never above the creative's remaining budget.
     */
    private double computeBidPrice(BidRequest request, PacingState pacing) {
        double floor = request.floorPrice();
        BidderProperties.Strategy s = properties.getStrategy();

        double base;
        // Prefer the fuller market window (wins AND losses) once we have enough samples —
        // loss clearing prices reveal what it actually costs to win, so this is a far less
        // biased estimate than win prices alone (which only sample at/below what we already
        // win). Fall back to the win-only average, then to cold-start, as samples thin out.
        double marketAvg = statsCache.getMarketSampleCount() >= s.getMinSamples()
                ? statsCache.getRollingAverageMarketPrice()
                : (statsCache.getSampleCount() >= s.getMinSamples()
                        ? statsCache.getRollingAverageWinPrice()
                        : 0.0);
        if (marketAvg <= 0.0) {
            // Not enough observed clearing prices to trust the market signal yet.
            base = floor * s.getColdStartMultiplier();
        } else {
            // Bid just over the observed market: low when we win easily, higher when we lose.
            double market = Math.max(floor, marketAvg);
            base = market * s.getMarketMultiplier();
        }

        base *= pacingMultiplier(pacing);

        // Never overspend a creative in a single win, but always stay strictly above the floor.
        double floorGuard = floor * 1.15;
        double capped = Math.min(base, Math.max(pacing.remainingBudget(), floorGuard));
        double price = Math.max(capped, floorGuard);
        // Round to 4 dp; re-guard in case rounding nudged us to the floor.
        price = Math.round(price * 10_000.0) / 10_000.0;
        return price <= floor ? floorGuard : price;
    }

    /**
     * A snapshot of where we are against the pacing plan for one bid: how far into the window we
     * are, how much budget we have already spent, and the remaining budget of the chosen creative.
     * The back-loaded target-spend curve and both pacing controls (price multiplier + throttle)
     * are derived from this, so they always agree on the same elapsed/spent view.
     */
    private record PacingState(double elapsedFraction, double spentFraction, double remainingBudget) {}

    private PacingState pacingState(double remainingBudget) {
        BidderProperties.Competition comp = properties.getCompetition();
        // Resolve the pacing start: an explicit competition.start-time wins; otherwise fall back
        // to the persisted pacing anchor (Redis-backed boot instant). Pacing is therefore always
        // active — a blank or unparseable start-time no longer silently disables it.
        Instant startTime;
        String configured = comp.getStartTime();
        if (configured != null && !configured.isBlank()) {
            try {
                startTime = Instant.parse(configured);
            } catch (Exception e) {
                startTime = pacingAnchor;
            }
        } else {
            startTime = pacingAnchor;
        }
        double elapsedSeconds = Duration.between(startTime, Instant.now()).toMillis() / 1000.0;
        double elapsedFraction = Math.max(0.0, Math.min(1.0, elapsedSeconds / comp.getDurationSeconds()));

        double fullBudget = properties.getCreativeBudget();
        double spentFraction = fullBudget <= 0 ? 0.0
                : Math.max(0.0, Math.min(1.0, 1.0 - remainingBudget / fullBudget));

        return new PacingState(elapsedFraction, spentFraction, remainingBudget);
    }

    /** Back-loaded target spend at this point in the window: elapsedFraction ^ curveExponent. */
    private double targetSpent(double elapsedFraction) {
        return Math.pow(elapsedFraction, properties.getStrategy().getPacingCurveExponent());
    }

    /**
     * Per-creative remaining-budget snapshot for the bid hot path. Refreshed at most once per
     * second, and the refresh runs OFF the event loop (subscribed on the bounded-elastic scheduler)
     * so a bid never blocks on Redis — it always reads the current, possibly-stale, snapshot
     * synchronously. On a refresh failure the previous snapshot is retained. lastBudgetFetch is
     * stamped before dispatching so only one refresh is ever in flight at a time.
     */
    private Map<String, Double> getBudgetSnapshot() {
        long now = System.currentTimeMillis();
        if (now - lastBudgetFetch > 1000) {
            lastBudgetFetch = now;
            getRemainingBudgets()
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(
                            fresh -> budgetSnapshot = fresh,
                            e -> log.warn("Budget snapshot refresh failed — keeping stale snapshot (non-fatal): {}",
                                    e.getMessage()));
        }
        return budgetSnapshot;
    }

    /** Record cleared spend (from a confirmed win) so linear pacing tracks live spend. */
    public void recordSpend(double amount) {
        totalSpentCents.addAndGet((long) (amount * 100));
    }

    /**
     * Linear pacing gate: bid only while our actual cleared spend is below the EVEN target spend
     * for the elapsed fraction of the competition window (times a small tolerance so we stay
     * mildly ahead). This spreads spend across the whole window — a steady fill — rather than
     * banking budget for the endgame. Elapsed fraction reuses the existing pacing anchor/start-time
     * resolution via pacingState().
     */
    private boolean shouldBid() {
        double elapsedFraction = pacingState(0).elapsedFraction();
        int creativeCount = budgetSnapshot.isEmpty() ? 200 : budgetSnapshot.size();
        double totalBudget = properties.getCreativeBudget() * creativeCount;
        double expectedSpend = elapsedFraction * totalBudget;
        double actualSpend = totalSpentCents.get() / 100.0;
        return actualSpend < expectedSpend * PACING_TOLERANCE;
    }

    private double pacingMultiplier(PacingState p) {
        BidderProperties.Strategy s = properties.getStrategy();
        if (p.elapsedFraction() >= s.getThrottleReleaseFraction() && p.spentFraction() < 1.0) {
            return s.getPacingBoost();
        }
        double gap = targetSpent(p.elapsedFraction()) - p.spentFraction();
        double multiplier = 1.0 + s.getPacingSensitivity() * gap;
        return Math.max(s.getPacingCut(), Math.min(s.getPacingBoost(), multiplier));
    }

    /** How specific a creative's targeting is: count of non-wildcard dimensions (0–3). */
    private int specificity(Creative creative) {
        int count = 0;
        if (creative.getAllowedGeos() != null && !creative.getAllowedGeos().isBlank()) count++;
        if (creative.getAllowedDevices() != null && !creative.getAllowedDevices().isBlank()) count++;
        if (creative.getAudienceSegments() != null && !creative.getAudienceSegments().isBlank()) count++;
        return count;
    }

    /** Milliseconds elapsed since a System.nanoTime() start marker. */
    private int elapsedMs(long startNanos) {
        return (int) ((System.nanoTime() - startNanos) / 1_000_000);
    }

    /** Total remaining budget across all this bidder's creatives. */
    public Mono<Double> getRemainingBudget() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()))
                .reduce(0.0, Double::sum);
    }

    /** Remaining budget per creative id. */
    public Mono<Map<String, Double>> getRemainingBudgets() {
        return creativeCache.getAll()
                .flatMap(c -> statsCache.getRemainingBudget(c.getId()).map(budget -> Map.entry(c.getId(), budget)))
                .collectMap(Map.Entry::getKey, Map.Entry::getValue, LinkedHashMap::new);
    }

    private CreativeDto toCreativeDto(Creative creative) {
        return new CreativeDto(
                creative.getId(),
                creative.getName(),
                creative.getDescription(),
                creative.getImageUrl(),
                creative.getCallToAction(),
                splitCsv(creative.getAllowedGeos()),
                splitCsv(creative.getAllowedDevices()),
                splitCsv(creative.getAudienceSegments())
        );
    }

    private List<String> splitCsv(String csv) {
        if (csv == null || csv.isBlank()) return List.of();
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    private BidRecord buildRecord(BidRequest request) {
        BidRecord record = new BidRecord();
        record.setRequestId(request.requestId());
        record.setFloorPrice(request.floorPrice());
        if (request.targeting() != null) {
            record.setGeo(request.targeting().geo());
            record.setDeviceType(request.targeting().deviceType());
            record.setAudienceSegment(request.targeting().audienceSegment());
        }
        return record;
    }
}

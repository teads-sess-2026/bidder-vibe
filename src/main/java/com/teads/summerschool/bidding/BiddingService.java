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

    // Last successfully computed budget.remaining, served when a scrape's
    // computation times out instead of blocking the scrape thread forever.
    private volatile double lastKnownBudget = 0.0;

    // When this JVM started, the fallback pacing anchor if none is configured or persisted yet.
    private final Instant bootInstant = Instant.now();
    // The resolved pacing anchor (persisted in Redis, so stable across restarts). Set once at
    // startup in registerBudgetGauge(); used by pacingMultiplier when no explicit start-time is set.
    private volatile Instant pacingAnchor = bootInstant;

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

        // Resolve the pacing anchor once at startup. setIfAbsent in Redis means the first boot's
        // instant is kept across restarts, so pacing always has a stable start to measure from —
        // it no longer silently disables itself just because competition.start-time is unset.
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

    /**
     * getRemainingBudget() does a DB query plus one Redis call per creative — under
     * DB/Redis pool contention (e.g. remote backing services with WAN latency) it can
     * queue for a connection indefinitely. /actuator/prometheus has no timeout of its
     * own, so an unbounded gauge supplier here stalls the entire scrape response.
     * Bound it the same way /api/bid bounds biddingService.bid(), and fall back to the
     * last known value instead of blocking Prometheus forever.
     *
     * <p>Micrometer's Gauge contract takes a plain synchronous Supplier<Number>, polled by the
     * Prometheus scrape thread — there's no reactive variant, so this is the one sanctioned
     * .block() outside of startup/Kafka-listener boundaries elsewhere in this codebase.
     */
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

    /**
     * Efficiency-first bidding: bid only on eligible creatives and only as much as it takes
     * to win. Eligibility follows CONFLUENCE F1–F3 order — max-bid cap, then targeting, then
     * remaining budget — and among eligible creatives we pick the most specific (least
     * wildcarded) one, tie-breaking on remaining budget so spend spreads out.
     */
    public Mono<Optional<BidResponse>> bid(BidRequest request) {
        long start = System.nanoTime();
        metrics.recordRequest();
        BidRecord record = buildRecord(request);
        double floor = request.floorPrice();

        // A null targeting block is treated as fully unrestricted (matches wildcard creatives).
        String geo = request.targeting() == null ? null : request.targeting().geo();
        String deviceType = request.targeting() == null ? null : request.targeting().deviceType();
        String audienceSegment = request.targeting() == null ? null : request.targeting().audienceSegment();

        return creativeCache.getAll().collectList().flatMap(all -> {
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
            // F2: only creatives that still have budget (> 0). One Redis read per matched
            // creative, run concurrently; carry the remaining budget along for ranking + pacing.
            return Flux.fromIterable(matched)
                    .flatMap(c -> statsCache.getRemainingBudget(c.getId())
                            .map(budget -> Map.entry(c, budget)))
                    .filter(e -> e.getValue() > 0)
                    .collectList()
                    .flatMap(eligible -> {
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
                        double bidPrice = computeBidPrice(request, creative, remaining);

                        // Let the win-notice consumer attribute a win to this bid without a DB hit.
                        ownBidCache.record(request.requestId(), creative.getId(), bidPrice);

                        record.setBidPrice(bidPrice);
                        record.setCreativeId(creative.getId());
                        record.setLatencyMs(elapsedMs(start));
                        metrics.recordBid();
                        metrics.recordLatency(elapsedMs(start));

                        BidResponse response = new BidResponse(
                                request.requestId(), bidPrice, toCreativeDto(creative));
                        return bidRecordRepository.save(record).thenReturn(Optional.of(response));
                    });
        });
    }

    /** Record a no-bid with its reason + latency and return an empty (204) response. */
    private Mono<Optional<BidResponse>> noBid(BidRecord record, String reason, long start) {
        record.setNoBidReason(reason);
        record.setLatencyMs(elapsedMs(start));
        metrics.recordNoBid(reason);
        metrics.recordLatency(elapsedMs(start));
        return bidRecordRepository.save(record).thenReturn(Optional.empty());
    }

    /**
     * Efficiency-first price: estimate the market clearing price and bid just enough over it.
     * Cold start (too few observed wins) falls back to a small markup over the floor. A pacing
     * multiplier then scales the bid up or down to keep each creative's budget alive across the
     * whole competition window — this is the sole price modifier. Always kept strictly above the
     * floor and never above the creative's remaining budget.
     */
    private double computeBidPrice(BidRequest request, Creative creative, double remainingBudget) {
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

        base *= pacingMultiplier(creative, remainingBudget);

        // Never overspend a creative in a single win, but always stay strictly above the floor.
        double floorGuard = floor * 1.02;
        double capped = Math.min(base, Math.max(remainingBudget, floorGuard));
        double price = Math.max(capped, floorGuard);
        // Round to 4 dp; re-guard in case rounding nudged us to the floor.
        price = Math.round(price * 10_000.0) / 10_000.0;
        return price <= floor ? floorGuard : price;
    }

    private double pacingMultiplier(Creative creative, double remainingBudget) {
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
        double elapsedFraction = elapsedSeconds / comp.getDurationSeconds();
        elapsedFraction = Math.max(0.0, Math.min(1.0, elapsedFraction));

        double fullBudget = properties.getCreativeBudget();
        double spentFraction = fullBudget <= 0 ? 0.0
                : Math.max(0.0, Math.min(1.0, 1.0 - remainingBudget / fullBudget));

        BidderProperties.Strategy s = properties.getStrategy();
        // Continuous, proportional response: +gap when under-pacing, -gap when over-pacing.
        double gap = elapsedFraction - spentFraction;
        double multiplier = 1.0 + s.getPacingSensitivity() * gap;
        // Clamp so a big gap can't send the bid to an absurd multiple or below the floor guard.
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

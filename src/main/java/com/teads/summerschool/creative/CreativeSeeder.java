package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.record.BidderStatsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class CreativeSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CreativeSeeder.class);

    private static final String[] GEOS     = {"US", "DE", "FR", "GB", "ES", "IT", "NL", "SE", "PL", "BR"};
    private static final String[] DEVICES  = {"mobile", "desktop", "tablet"};
    private static final String[] SEGMENTS = {"sports", "tech", "fashion", "gaming", "travel", "food", "finance", "health"};

    // 200 creatives × bidder.creative-budget ($25) = the whole $5,000 pool.
    private static final int  CREATIVE_COUNT = 200;
    private static final long SEED           = 42;

    // Targeting is WIDE on purpose. geo/device/segment carry no value signal in this competition
    // (see the winning strategy note below), so any restriction only subtracts coverage and strands
    // that creative's $25 in a slice it rarely matches — which is what left 43% of the budget
    // unspent last run. High wildcard probability + broad subsets keep almost every creative eligible
    // for almost every auction, so pacing can actually drain all 200 pools. No maxBidPrice cap for the
    // same reason: a low cap filtered creatives out of most auctions and stranded their budget.
    private static final double WILDCARD_PROBABILITY = 0.70;

    private final CreativeRepository repository;
    private final BidderProperties   properties;
    private final BidderStatsCache   statsCache;
    private final CreativeCache      creativeCache;

    public CreativeSeeder(CreativeRepository repository, BidderProperties properties,
                          BidderStatsCache statsCache, CreativeCache creativeCache) {
        this.repository    = repository;
        this.properties    = properties;
        this.statsCache    = statsCache;
        this.creativeCache = creativeCache;
    }

    @Override
    public void run(ApplicationArguments args) {
        String id = properties.getId();

        // Deterministic set of 200 mostly-wildcard creatives (fixed SEED = same catalog every boot).
        Random rnd = new Random(SEED);
        List<Creative> seedCreatives = new ArrayList<>(CREATIVE_COUNT);
        for (int i = 1; i <= CREATIVE_COUNT; i++) {
            seedCreatives.add(creative(id + "-creative-" + i, "Creative " + i,
                    "Auto-generated creative #" + i,
                    pickSubset(GEOS, rnd), pickSubset(DEVICES, rnd), pickSubset(SEGMENTS, rnd)));
        }

        // Seed AND initialize budgets only on the first boot (no existing rows). On a restart or
        // mid-competition redeploy we skip both. The SSP is the single owner of budget spend, so
        // initBudget only ever seeds with setIfAbsent — even a re-run can't refill an already-spent
        // key. Safe if Redis was wiped but Postgres kept the rows: getRemainingBudget lazily
        // re-inits a missing key (setIfAbsent) to the flat creative budget.
        repository.findByBidderId(id).hasElements()
                .flatMap(hasAny -> {
                    if (hasAny) {
                        log.info("Creatives already seeded — preserving live budgets, skipping re-init.");
                        return Mono.empty();
                    }
                    return repository.saveAll(seedCreatives)
                            .flatMap(c -> statsCache.initBudget(c.getId(), c.getBudget())
                                    .onErrorResume(e -> {
                                        log.warn("Failed to seed budget for creative {} — will lazy-init on first read: {}",
                                                c.getId(), e.getMessage());
                                        return Mono.empty();
                                    }))
                            .then();
                })
                // Drop any stale cached catalog so the first read repopulates from the seeded rows —
                // invalidation is driven by this write path, not a TTL.
                .then(creativeCache.invalidate())
                .then(Mono.defer(creativeCache::refresh))
                .block();
    }

    /**
     * Pick a WIDE targeting value for one dimension: most of the time no restriction at all
     * (wildcard), and when restricted, a broad subset (at least half the options) so the creative
     * still matches a large share of auctions. Widened from the old 0.3 wildcard / 1..N subset that
     * stranded budget in narrowly-targeted creatives.
     */
    private String pickSubset(String[] options, Random rnd) {
        if (rnd.nextDouble() < WILDCARD_PROBABILITY) return "";
        int half = Math.max(1, options.length / 2);
        int n = half + rnd.nextInt(options.length - half + 1); // at least half the options
        List<String> pool = new ArrayList<>(List.of(options));
        Collections.shuffle(pool, rnd);
        return String.join(",", pool.subList(0, n));
    }

    private Creative creative(String creativeId, String name, String description,
                               String geos, String devices, String segments) {
        Creative c = new Creative();
        c.setId(creativeId);
        c.setName(name);
        c.setDescription(description);
        c.setImageUrl("https://placeholder.com/" + creativeId + ".jpg");
        c.setCallToAction("Learn More");
        c.setBidderId(properties.getId());
        c.setAllowedGeos(geos);
        c.setAllowedDevices(devices);
        c.setAudienceSegments(segments);
        c.setBudget(properties.getCreativeBudget());
        // No maxBidPrice cap: leave unbounded so a creative is never filtered out of an auction it
        // otherwise matches. A cap only strands budget in this no-value-signal competition.
        return c;
    }
}

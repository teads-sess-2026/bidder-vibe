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
import reactor.core.publisher.Mono;

import java.util.List;

@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
public class CreativeSeeder implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(CreativeSeeder.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final CreativeCache creativeCache;

    public CreativeSeeder(CreativeRepository repository, BidderProperties properties,
                           BidderStatsCache statsCache, CreativeCache creativeCache) {
        this.repository = repository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.creativeCache = creativeCache;
    }

    @Override
    public void run(ApplicationArguments args) {
        String id = properties.getId();

        // Coverage-first creative pool (same count as before, so total deployable
        // budget is unchanged — this is a coverage fix, not a budget grab). Wins are
        // the leaderboard currency, so every request we can legally serve is a chance
        // to win; narrow targeting just turns into no-bids once our one wildcard
        // creative exhausts. We therefore run TWO fully-wildcard creatives as broad
        // workhorses — the bidder tie-breaks equally-specific matches on remaining
        // budget, so spend spreads across both and broad coverage lasts ~twice as
        // long. The two targeted creatives widen their old restrictions (dropping the
        // over-narrow segment filters and the 0.30 cap that priced US traffic out) so
        // they add coverage rather than subtract it.
        //
        // TODO: tune targeting (geos, devices, segments) and maxBidPrice (highest floor
        // this creative will bid on; null = unbounded) to match your strategy.
        List<Creative> seedCreatives = List.of(
                creative(id + "-creative-1", "Universal",        "No restrictions — serves everywhere",     "", "", ""),
                creative(id + "-creative-2", "Universal Reserve","Second wildcard pool for broad coverage", "", "", ""),
                creative(id + "-creative-3", "EU Broad",         "European markets, all devices/segments",  "DE,FR,GB,ES,IT,NL", "", ""),
                creative(id + "-creative-4", "US Broad",         "US inventory, all devices/segments",      "US", "", "")
        );

        repository.findByBidderId(id).hasElements()
                .flatMap(hasAny -> hasAny ? Mono.empty() : repository.saveAll(seedCreatives).then())
                .thenMany(repository.findByBidderId(id))
                // Reset each creative's remaining budget in Redis to its configured limit on startup.
                // A transient Redis timeout on one creative shouldn't crash the whole app: getRemainingBudget
                // already falls back to the flat creative budget for a missing key, and recordWin's
                // setIfAbsent lazily initializes it on first win, so skipping a failed creative here is
                // safe, not silently wrong.
                .flatMap(c -> statsCache.initBudget(c.getId(), c.getBudget())
                        .onErrorResume(e -> {
                            log.warn("Failed to init budget for creative {} — will lazy-init on first read/win: {}",
                                    c.getId(), e.getMessage());
                            return Mono.empty();
                        }))
                // Drop any stale cached catalog so the first read repopulates from the rows we
                // just seeded — invalidation is driven by this write path, not a TTL.
                .then(creativeCache.invalidate())
                .then(Mono.defer(creativeCache::refresh))
                .block();
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
        return c;
    }
}

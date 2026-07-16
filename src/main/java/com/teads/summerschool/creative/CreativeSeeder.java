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

        // A SINGLE fully-wildcard creative holding the whole budget. Wins are the leaderboard
        // currency and geo/device/segment carry NO value signal in this competition, so any
        // targeting restriction can only subtract coverage; one wildcard pool serves everywhere.
        // Consolidating from four identical wildcards to one is a latency + pacing fix, not a
        // budget change: total deployable budget is still bidder.creative-budget (raised to hold
        // what the four pools held together). Benefits — one Redis budget GET per bid instead of
        // up to four (lower tail latency, fewer timeouts), and pacing steers one whole budget so
        // no fraction gets stranded in a pool that never drains.
        //
        // TODO: tune maxBidPrice (highest floor this creative will bid on; null = unbounded) to
        // match your strategy. Leave targeting wildcard unless a value signal appears.
        List<Creative> seedCreatives = List.of(
                creative(id + "-creative-1", "Universal", "No restrictions — serves everywhere", "", "", "")
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

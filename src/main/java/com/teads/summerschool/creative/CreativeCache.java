package com.teads.summerschool.creative;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.teads.summerschool.config.BidderProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

/**
 * Lookup for this bidder's creative catalog.
 *
 * <p>The catalog is read on every {@code /api/bid} and every {@code /api/budget} poll
 * (the SSP polls budget ~1×/sec), so a Postgres round trip per call was the dominant DB
 * cost under load. This is now a cache-aside read against Redis: the catalog is stored
 * as a JSON blob under {@code {bidderId}_creatives} and only reloaded from Postgres on a
 * miss. Any Redis error falls back to Postgres, so a Redis blip degrades to the old
 * behavior rather than failing bids.
 *
 * <p>Invalidation is write-driven, not time-driven — {@link #invalidate()} is called by
 * the write path (see CreativeSeeder) so a stale snapshot can never hide a just-added
 * creative or keep matching a removed one. A short TTL is layered on only as a safety
 * net for a missed invalidation.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    // Safety net for a missed write-path invalidation; short enough to self-heal quickly.
    private static final Duration TTL = Duration.ofSeconds(60);

    // Refresh interval for the in-memory catalog snapshot below. The catalog is effectively static
    // after seeding, so a long interval keeps background Redis reads rare.
    private static final long MEM_REFRESH_MS = 15_000;

    // In-memory catalog snapshot read synchronously on the bid hot path — no Redis GET and no
    // 200-creative JSON deserialize per bid. Refreshed at most every MEM_REFRESH_MS off the event
    // loop; primed at startup so the first bid already has it.
    private volatile List<Creative> memSnapshot = List.of();
    private volatile long lastMemFetch = 0;

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final ReactiveRedisTemplate<String, String> redis;
    // Self-contained mapper: WebFlux doesn't reliably expose a standalone ObjectMapper bean,
    // and this only serializes the plain Creative POJO to/from a cache blob. Ignore unknown
    // properties so the serialized "new" flag (from Persistable.isNew(), which has no setter)
    // round-trips without blowing up on read.
    private final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public CreativeCache(CreativeRepository repository, BidderProperties properties,
                          ReactiveRedisTemplate<String, String> redis) {
        this.repository = repository;
        this.properties = properties;
        this.redis = redis;
    }

    private String cacheKey() {
        return properties.getId() + "_creatives";
    }

    public Flux<Creative> getAll() {
        String key = cacheKey();
        return redis.opsForValue().get(key)
                .flatMapMany(this::deserialize)
                // Cache miss: load from Postgres, then populate Redis for the next read.
                .switchIfEmpty(loadFromDbAndCache(key))
                // Any Redis failure: never fail the bid — fall straight back to Postgres.
                .onErrorResume(e -> {
                    log.warn("Creative cache read failed, falling back to Postgres: {}", e.getMessage());
                    return repository.findByBidderId(properties.getId());
                });
    }

    /**
     * Synchronous, allocation-free read of the catalog for the bid hot path: returns the
     * in-memory snapshot with NO Redis GET and NO JSON deserialize per bid. At most once every
     * {@link #MEM_REFRESH_MS} it kicks off a background refresh (off the event loop via
     * boundedElastic) from {@link #getAll()} — which itself is Redis-cache-aside with a Postgres
     * fallback — and updates the snapshot when it completes. A refresh failure keeps the stale
     * snapshot. The snapshot is primed at startup (see {@link #primeMemSnapshot()}) so the very
     * first bid already has the catalog and never blocks.
     */
    public List<Creative> getAllCached() {
        long now = System.currentTimeMillis();
        // Refresh on the interval, OR immediately while the snapshot is still empty — the latter
        // covers first boot, where CreativeSeeder (an ApplicationRunner) seeds AFTER this bean's
        // @PostConstruct prime, so the prime can see an empty DB. Once populated, only the interval
        // triggers refreshes.
        if (memSnapshot.isEmpty() || now - lastMemFetch > MEM_REFRESH_MS) {
            lastMemFetch = now;
            getAll().collectList()
                    .subscribeOn(Schedulers.boundedElastic())
                    .subscribe(
                            list -> memSnapshot = list,
                            e -> log.warn("Creative mem-snapshot refresh failed — keeping stale (non-fatal): {}",
                                    e.getMessage()));
        }
        return memSnapshot;
    }

    /**
     * Prime the in-memory catalog snapshot once at startup so the first bid is already served from
     * memory. Bounded blocking is fine here — it runs on the startup thread, not the event loop.
     */
    @PostConstruct
    void primeMemSnapshot() {
        try {
            List<Creative> list = getAll().collectList().block(Duration.ofSeconds(10));
            if (list != null) {
                memSnapshot = list;
                lastMemFetch = System.currentTimeMillis();
                log.info("Creative catalog primed in memory: {} creatives", list.size());
            }
        } catch (Exception e) {
            // Non-fatal: leave the snapshot empty; the first getAllCached() call will trigger a
            // background refresh, and BiddingService treats an empty catalog as a no-bid.
            log.warn("Creative catalog prime failed (non-fatal), will refresh lazily: {}", e.getMessage());
        }
    }

    private Flux<Creative> loadFromDbAndCache(String key) {
        return repository.findByBidderId(properties.getId())
                .collectList()
                .flatMapMany(creatives -> writeCache(key, creatives)
                        .thenMany(Flux.fromIterable(creatives)));
    }

    private Flux<Creative> deserialize(String json) {
        try {
            List<Creative> creatives = objectMapper.readValue(
                    json, objectMapper.getTypeFactory().constructCollectionType(List.class, Creative.class));
            return Flux.fromIterable(creatives);
        } catch (Exception e) {
            // Corrupt/incompatible cache entry — treat as a miss so the DB path repopulates it.
            log.warn("Creative cache deserialize failed, treating as miss: {}", e.getMessage());
            return Flux.empty();
        }
    }

    private Mono<Boolean> writeCache(String key, List<Creative> creatives) {
        try {
            String json = objectMapper.writeValueAsString(creatives);
            return redis.opsForValue().set(key, json, TTL)
                    .onErrorResume(e -> {
                        log.warn("Creative cache write failed (non-fatal): {}", e.getMessage());
                        return Mono.just(false);
                    });
        } catch (Exception e) {
            log.warn("Creative cache serialize failed (non-fatal): {}", e.getMessage());
            return Mono.just(false);
        }
    }

    /** Drop the cached catalog so the next read repopulates from Postgres. Call from the write path. */
    public Mono<Void> invalidate() {
        return redis.opsForValue().delete(cacheKey())
                .doOnNext(deleted -> log.info("Creative cache invalidated: {}", cacheKey()))
                .onErrorResume(e -> {
                    log.warn("Creative cache invalidate failed (non-fatal): {}", e.getMessage());
                    return Mono.just(false);
                })
                .then();
    }

    /** Kept for CreativeSeeder, which logs the catalog size right after seeding. */
    public Mono<Void> refresh() {
        return getAll().count()
                .doOnNext(n -> log.info("Creative catalog seeded: {} creatives", n))
                .then();
    }
}

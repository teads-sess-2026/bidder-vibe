package com.teads.summerschool.creative;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

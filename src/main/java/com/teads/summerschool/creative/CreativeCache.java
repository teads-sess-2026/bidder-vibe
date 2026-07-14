package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Lookup for this bidder's creative catalog. Originally snapshotted the catalog once
 * (right after CreativeSeeder seeded it) to save a Postgres round trip per bid() call,
 * but creatives can be added/removed after startup faster than any cache could track —
 * a stale snapshot then either hides a just-added creative or keeps matching one that's
 * already gone. getAll() reads straight from Postgres each time to stay correct;
 * revisit only with a cache invalidated by the write path itself, not time.
 */
@Component
public class CreativeCache {

    private static final Logger log = LoggerFactory.getLogger(CreativeCache.class);

    private final CreativeRepository repository;
    private final BidderProperties properties;

    public CreativeCache(CreativeRepository repository, BidderProperties properties) {
        this.repository = repository;
        this.properties = properties;
    }

    public List<Creative> getAll() {
        return repository.findByBidderId(properties.getId());
    }

    /** Kept for CreativeSeeder, which logs the catalog size right after seeding. */
    public void refresh() {
        log.info("Creative catalog seeded: {} creatives", getAll().size());
    }
}

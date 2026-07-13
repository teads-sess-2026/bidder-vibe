package com.teads.summerschool.creative;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.record.BidderStatsCache;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CreativeSeeder implements ApplicationRunner {

    private final CreativeRepository repository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;

    public CreativeSeeder(CreativeRepository repository, BidderProperties properties, BidderStatsCache statsCache) {
        this.repository = repository;
        this.properties = properties;
        this.statsCache = statsCache;
    }

    @Override
    public void run(ApplicationArguments args) {
        String id = properties.getId();
        if (repository.findByBidderId(id).isEmpty()) {
            // TODO: customize your creatives — adjust targeting (geos, devices, segments) to match your strategy
            repository.saveAll(List.of(
                creative(id + "-creative-1", "Universal",    "No restrictions — serves everywhere",   "", "", ""),
                creative(id + "-creative-2", "Mobile Tech",  "Mobile-optimized for tech audience",     "", "mobile", "tech,gaming"),
                creative(id + "-creative-3", "EU Premium",   "European markets, all devices",          "DE,FR,GB,ES,IT,NL", "", ""),
                creative(id + "-creative-4", "US Sports",    "US sports audience",                     "US", "", "sports,health")
            ));
        }

        // Reset each creative's remaining budget in Redis to its configured limit on startup.
        for (Creative c : repository.findByBidderId(id)) {
            statsCache.initBudget(c.getId(), c.getBudget());
        }
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

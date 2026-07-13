package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.bidding.dto.BidResponse;
import com.teads.summerschool.bidding.dto.CreativeDto;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeRepository;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class BiddingService {

    private static final Logger log = LoggerFactory.getLogger(BiddingService.class);

    private final BidderProperties properties;
    private final CreativeRepository creativeRepository;
    private final BidRecordRepository bidRecordRepository;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;

    public BiddingService(BidderProperties properties,
                          CreativeRepository creativeRepository,
                          BidRecordRepository bidRecordRepository,
                          BidderStatsCache statsCache,
                          BidderMetrics metrics) {
        this.properties = properties;
        this.creativeRepository = creativeRepository;
        this.bidRecordRepository = bidRecordRepository;
        this.statsCache = statsCache;
        this.metrics = metrics;
    }

    @PostConstruct
    void registerBudgetGauge() {
        metrics.registerGauge("budget.remaining", this::getRemainingBudget);
    }

    @Transactional
    public Optional<BidResponse> bid(BidRequest request) {
        // TODO: implement your bidding strategy
        // Hints:
        //   1. Record the request with buildRecord(request)
        //   2. Find matching creatives with matchingCreatives(request)
        //   3. Filter creatives that still have budget: statsCache.getRemainingBudget(c.getId()) > 0
        //   4. Compute a bid price with computeBidPrice(request)
        //   5. Record metrics: metrics.recordRequest(), metrics.recordBid(), metrics.recordNoBid(reason)
        //   6. Save the BidRecord and return Optional.of(new BidResponse(...)) or Optional.empty()
        metrics.recordRequest();
        metrics.recordNoBid("not_implemented");
        BidRecord record = buildRecord(request);
        record.setNoBidReason("not_implemented");
        long start = System.nanoTime();
        record.setLatencyMs((int) ((System.nanoTime() - start) / 1_000_000));
        metrics.recordLatency(0);
        bidRecordRepository.save(record);
        return Optional.empty();
    }

    private double computeBidPrice(BidRequest request) {
        // TODO: implement your pricing strategy
        // The bid must be above request.floorPrice().
        // Use properties.getStrategy() for tuning parameters.
        return request.floorPrice() * 1.01;
    }

    /** Total remaining budget across all this bidder's creatives. */
    public double getRemainingBudget() {
        return creativeRepository.findByBidderId(properties.getId()).stream()
                .mapToDouble(c -> statsCache.getRemainingBudget(c.getId()))
                .sum();
    }

    /** Remaining budget per creative id. */
    public Map<String, Double> getRemainingBudgets() {
        Map<String, Double> budgets = new LinkedHashMap<>();
        for (Creative c : creativeRepository.findByBidderId(properties.getId())) {
            budgets.put(c.getId(), statsCache.getRemainingBudget(c.getId()));
        }
        return budgets;
    }

    private List<Creative> matchingCreatives(BidRequest request) {
        return creativeRepository.findByBidderId(properties.getId()).stream()
                .filter(c -> c.matches(
                        request.targeting().geo(),
                        request.targeting().deviceType(),
                        request.targeting().audienceSegment()))
                .toList();
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

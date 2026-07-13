package com.teads.summerschool.bidding;

import com.teads.summerschool.bidding.dto.BidRequest;
import com.teads.summerschool.config.BidderProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@RestController
public class BidController {

    private static final Logger log = LoggerFactory.getLogger(BidController.class);

    private final BiddingService biddingService;
    private final BidderProperties properties;

    public BidController(BiddingService biddingService, BidderProperties properties) {
        this.biddingService = biddingService;
        this.properties = properties;
    }

    @PostMapping("/api/bid")
    public CompletableFuture<ResponseEntity<?>> bid(@RequestBody BidRequest request) {
        return CompletableFuture
                .supplyAsync(() -> biddingService.bid(request))
                .orTimeout(properties.getTimeoutMs(), TimeUnit.MILLISECONDS)
                .thenApply(opt -> {
                    if (opt.isPresent()) {
                        return (ResponseEntity<?>) ResponseEntity.ok(opt.get());
                    }
                    return (ResponseEntity<?>) ResponseEntity.noContent().build();
                })
                .exceptionally(ex -> {
                    log.warn("<< BID TIMEOUT  id={} — {}", request.requestId(), ex.getMessage());
                    return ResponseEntity.noContent().build();
                });
    }

    @GetMapping("/api/budget")
    public ResponseEntity<Map<String, Object>> budget() {
        return ResponseEntity.ok(Map.of(
                "remaining", biddingService.getRemainingBudget(),
                "creatives", biddingService.getRemainingBudgets()
        ));
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, String>> health() {
        return ResponseEntity.ok(Map.of("status", "UP"));
    }
}

package com.teads.summerschool.stats;

import com.teads.summerschool.stats.dto.CreativeStatsResponse;
import com.teads.summerschool.stats.dto.StatsResponse;
import com.teads.summerschool.stats.dto.TargetingResponse;
import com.teads.summerschool.stats.dto.TimeseriesResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Set;

@RestController
@RequestMapping("/api/stats")
public class StatsController {

    private static final Set<String> VALID_SORT  = Set.of("spend", "wins", "bids", "bid_rate", "win_rate");
    private static final Set<String> VALID_ORDER = Set.of("asc", "desc");
    private static final Set<String> VALID_DIM   = Set.of("geo", "device", "segment", "all");

    private final StatsService statsService;

    public StatsController(StatsService statsService) {
        this.statsService = statsService;
    }

    /** GET /api/stats — overall snapshot */
    @GetMapping
    public Mono<ResponseEntity<StatsResponse>> stats() {
        return statsService.getStats().map(ResponseEntity::ok);
    }

    /** GET /api/stats/creatives?creative_id=&sort=spend&order=desc */
    @GetMapping("/creatives")
    public Mono<ResponseEntity<?>> creatives(
            @RequestParam(required = false) String creative_id,
            @RequestParam(defaultValue = "spend") String sort,
            @RequestParam(defaultValue = "desc") String order) {

        if (!VALID_SORT.contains(sort))  return Mono.just(ResponseEntity.badRequest().body(Map.of("error", "invalid sort: " + sort)));
        if (!VALID_ORDER.contains(order)) return Mono.just(ResponseEntity.badRequest().body(Map.of("error", "invalid order: " + order)));

        return statsService.getCreativeStats(creative_id, sort, order).map(ResponseEntity::ok);
    }

    /** GET /api/stats/targeting?dimension=all */
    @GetMapping("/targeting")
    public Mono<ResponseEntity<?>> targeting(
            @RequestParam(defaultValue = "all") String dimension) {

        if (!VALID_DIM.contains(dimension)) return Mono.just(ResponseEntity.badRequest().body(Map.of("error", "invalid dimension: " + dimension)));

        return statsService.getTargetingStats(dimension).map(ResponseEntity::ok);
    }

    /** GET /api/stats/timeseries?window_minutes=30&bucket_seconds=60 */
    @GetMapping("/timeseries")
    public Mono<ResponseEntity<TimeseriesResponse>> timeseries(
            @RequestParam(name = "window_minutes", defaultValue = "30") int windowMinutes,
            @RequestParam(name = "bucket_seconds", defaultValue = "60") int bucketSeconds) {

        return statsService.getTimeseries(windowMinutes, bucketSeconds).map(ResponseEntity::ok);
    }
}

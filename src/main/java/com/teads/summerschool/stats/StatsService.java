package com.teads.summerschool.stats;

import com.teads.summerschool.bidding.BiddingService;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.creative.Creative;
import com.teads.summerschool.creative.CreativeCache;
import com.teads.summerschool.notification.WinNotice;
import com.teads.summerschool.notification.WinNoticeRepository;
import com.teads.summerschool.record.BidRecord;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.stats.dto.CreativeStatsResponse;
import com.teads.summerschool.stats.dto.StatsResponse;
import com.teads.summerschool.stats.dto.TargetingResponse;
import com.teads.summerschool.stats.dto.TimeseriesResponse;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Read-only dashboard aggregation over the durable audit tables ({@code bid_record},
 * {@code win_notice}). Everything is reactive — these run on the Netty event loop, so no
 * {@code .block()}. Wins carry no creative id (the win notice only has request_id), so
 * per-creative/targeting attribution is done by joining win_notice.request_id back to the
 * bid_record we wrote for that auction.
 */
@Service
public class StatsService {

    private final BidderProperties properties;
    private final BidRecordRepository bidRecordRepository;
    private final WinNoticeRepository winNoticeRepository;
    private final BiddingService biddingService;
    private final CreativeCache creativeCache;

    public StatsService(BidderProperties properties,
                        BidRecordRepository bidRecordRepository,
                        WinNoticeRepository winNoticeRepository,
                        BiddingService biddingService,
                        CreativeCache creativeCache) {
        this.properties = properties;
        this.bidRecordRepository = bidRecordRepository;
        this.winNoticeRepository = winNoticeRepository;
        this.biddingService = biddingService;
        this.creativeCache = creativeCache;
    }

    // ── Overall snapshot ─────────────────────────────────────────────────────────

    public Mono<StatsResponse> getStats() {
        Mono<Long> totalRecords = bidRecordRepository.count();
        Mono<Long> bids = bidRecordRepository.countByBidPriceIsNotNull();
        Mono<Long> wins = winNoticeRepository.count();
        Mono<Double> spend = winNoticeRepository.sumClearingPrice().defaultIfEmpty(0.0);
        Mono<Double> avgBid = bidRecordRepository.avgBidPrice().defaultIfEmpty(0.0);
        Mono<Double> avgWin = winNoticeRepository.avgClearingPrice().defaultIfEmpty(0.0);
        Mono<List<BidRecordRepository.NoBidReasonCount>> reasons =
                bidRecordRepository.countGroupByNoBidReason().collectList();
        Mono<List<Integer>> latencies = bidRecordRepository.findAllLatenciesSorted().collectList();
        Mono<Double> remaining = biddingService.getRemainingBudget().defaultIfEmpty(0.0);
        Mono<LocalDateTime> firstAt = bidRecordRepository.findFirstCreatedAt()
                .map(t -> (LocalDateTime) t).defaultIfEmpty(LocalDateTime.now());

        List<Mono<?>> parts = List.of(totalRecords, bids, wins, spend, avgBid, avgWin,
                reasons, latencies, remaining, firstAt);

        return Mono.zip(parts, arr -> {
            long total = (Long) arr[0];
            long bidCount = (Long) arr[1];
            long winCount = (Long) arr[2];
            double spendVal = (Double) arr[3];
            double avgBidVal = (Double) arr[4];
            double avgWinVal = (Double) arr[5];
            @SuppressWarnings("unchecked")
            List<BidRecordRepository.NoBidReasonCount> reasonList =
                    (List<BidRecordRepository.NoBidReasonCount>) arr[6];
            @SuppressWarnings("unchecked")
            List<Integer> latencyList = (List<Integer>) arr[7];
            double remainingVal = (Double) arr[8];
            LocalDateTime first = (LocalDateTime) arr[9];

            long noBids = Math.max(0, total - bidCount);
            double bidRate = total > 0 ? (double) bidCount / total : 0.0;
            double winRate = bidCount > 0 ? (double) winCount / bidCount : 0.0;
            double winRatePerAuction = total > 0 ? (double) winCount / total : 0.0;

            Map<String, Integer> reasonMap = reasonList.stream()
                    .collect(Collectors.toMap(
                            r -> r.noBidReason() == null ? "" : r.noBidReason(),
                            r -> (int) r.cnt(), (a, b) -> a));

            StatsResponse.NoBidReasons noBidReasons = new StatsResponse.NoBidReasons(
                    reasonMap.getOrDefault("budget_exhausted", 0),
                    reasonMap.getOrDefault("no_eligible_creative", 0),
                    reasonMap.getOrDefault("targeting_miss", 0));

            StatsResponse.LatencyStats latencyStats = latencyStats(latencyList);

            double budget = remainingVal + spendVal;
            double elapsedMinutes = Math.max(0.0, Duration.between(first, LocalDateTime.now()).toMillis() / 60_000.0);
            double spendPerMinute = elapsedMinutes > 0 ? spendVal / elapsedMinutes : 0.0;
            Double projected = spendPerMinute > 0 ? remainingVal / spendPerMinute : null;
            double utilization = budget > 0 ? spendVal / budget : 0.0;
            StatsResponse.PacingStats pacing = new StatsResponse.PacingStats(
                    spendPerMinute, elapsedMinutes, projected, utilization);

            return new StatsResponse(
                    properties.getId(), LocalDateTime.now(),
                    total, bidCount, noBids, bidRate,
                    winCount, winRate, winRatePerAuction,
                    avgBidVal, avgWinVal, spendVal, remainingVal, budget,
                    latencyStats, noBidReasons, pacing);
        });
    }

    private StatsResponse.LatencyStats latencyStats(List<Integer> sorted) {
        if (sorted.isEmpty()) {
            return new StatsResponse.LatencyStats(0.0, 0, 0, 0, 0);
        }
        double avg = sorted.stream().mapToInt(Integer::intValue).average().orElse(0.0);
        int p50 = percentile(sorted, 0.50);
        int p95 = percentile(sorted, 0.95);
        int max = sorted.get(sorted.size() - 1);
        return new StatsResponse.LatencyStats(avg, p50, p95, max, sorted.size());
    }

    /** Nearest-rank percentile over an already-sorted list. */
    private int percentile(List<Integer> sorted, double q) {
        if (sorted.isEmpty()) return 0;
        int idx = (int) Math.ceil(q * sorted.size()) - 1;
        idx = Math.max(0, Math.min(sorted.size() - 1, idx));
        return sorted.get(idx);
    }

    // ── Per-creative breakdown ───────────────────────────────────────────────────

    public Mono<CreativeStatsResponse> getCreativeStats(String creativeId, String sort, String order) {
        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByBidPriceIsNotNull().collectList();
        Mono<List<WinNotice>> winsMono = winNoticeRepository.findAll().collectList();
        Mono<Map<String, String>> namesMono = creativeCache.getAll()
                .collectMap(Creative::getId, Creative::getName, LinkedHashMap::new);

        return Mono.zip(bidsMono, winsMono, namesMono).map(t -> {
            List<BidRecord> bids = t.getT1();
            List<WinNotice> wins = t.getT2();
            Map<String, String> names = t.getT3();

            // requestId -> creativeId, so wins (which lack a creative id) attribute correctly.
            Map<String, String> reqToCreative = bids.stream()
                    .filter(b -> b.getCreativeId() != null)
                    .collect(Collectors.toMap(BidRecord::getRequestId, BidRecord::getCreativeId, (a, b) -> a));

            Map<String, Agg> byCreative = new LinkedHashMap<>();
            for (BidRecord b : bids) {
                if (b.getCreativeId() == null) continue;
                Agg a = byCreative.computeIfAbsent(b.getCreativeId(), k -> new Agg());
                a.bids++;
                if (b.getBidPrice() != null) a.bidPriceSum += b.getBidPrice();
            }
            for (WinNotice w : wins) {
                String cid = reqToCreative.get(w.getRequestId());
                if (cid == null) continue;
                Agg a = byCreative.computeIfAbsent(cid, k -> new Agg());
                a.wins++;
                a.spend += w.getClearingPrice();
                a.winPriceSum += w.getClearingPrice();
            }

            List<CreativeStatsResponse.CreativeStat> stats = new ArrayList<>();
            for (Map.Entry<String, Agg> e : byCreative.entrySet()) {
                if (creativeId != null && !creativeId.isBlank() && !creativeId.equals(e.getKey())) continue;
                Agg a = e.getValue();
                double winRate = a.bids > 0 ? (double) a.wins / a.bids : 0.0;
                double avgBid = a.bids > 0 ? a.bidPriceSum / a.bids : 0.0;
                double avgWin = a.wins > 0 ? a.winPriceSum / a.wins : 0.0;
                stats.add(new CreativeStatsResponse.CreativeStat(
                        e.getKey(), names.getOrDefault(e.getKey(), e.getKey()),
                        a.bids, a.wins, winRate, avgBid, avgWin, a.spend));
            }

            sortCreatives(stats, sort, order);
            return new CreativeStatsResponse(properties.getId(), stats);
        });
    }

    private void sortCreatives(List<CreativeStatsResponse.CreativeStat> stats, String sort, String order) {
        Comparator<CreativeStatsResponse.CreativeStat> cmp = switch (sort) {
            case "wins" -> Comparator.comparingLong(CreativeStatsResponse.CreativeStat::wins);
            case "bids" -> Comparator.comparingLong(CreativeStatsResponse.CreativeStat::bids);
            case "bid_rate" -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::winRate);
            case "win_rate" -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::winRate);
            default -> Comparator.comparingDouble(CreativeStatsResponse.CreativeStat::spend);
        };
        if ("desc".equals(order)) cmp = cmp.reversed();
        stats.sort(cmp);
    }

    private static final class Agg {
        long bids;
        long wins;
        double bidPriceSum;
        double winPriceSum;
        double spend;
    }

    // ── Targeting breakdown ──────────────────────────────────────────────────────

    public Mono<TargetingResponse> getTargetingStats(String dimension) {
        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByBidPriceIsNotNull().collectList();
        Mono<Set<String>> winReqsMono = winNoticeRepository.findAll()
                .map(WinNotice::getRequestId).collect(Collectors.toSet());

        return Mono.zip(bidsMono, winReqsMono).map(t -> {
            List<BidRecord> bids = t.getT1();
            Set<String> winReqs = t.getT2();

            boolean all = "all".equals(dimension);
            List<TargetingResponse.TargetingBucket> byGeo =
                    (all || "geo".equals(dimension)) ? bucket(bids, winReqs, BidRecord::getGeo) : List.of();
            List<TargetingResponse.TargetingBucket> byDevice =
                    (all || "device".equals(dimension)) ? bucket(bids, winReqs, BidRecord::getDeviceType) : List.of();
            List<TargetingResponse.TargetingBucket> bySegment =
                    (all || "segment".equals(dimension)) ? bucket(bids, winReqs, BidRecord::getAudienceSegment) : List.of();

            return new TargetingResponse(properties.getId(), byGeo, byDevice, bySegment);
        });
    }

    private List<TargetingResponse.TargetingBucket> bucket(
            List<BidRecord> bids, Set<String> winReqs, Function<BidRecord, String> keyFn) {
        Map<String, Agg> byKey = new LinkedHashMap<>();
        for (BidRecord b : bids) {
            String key = keyFn.apply(b);
            if (key == null || key.isBlank()) key = "(none)";
            Agg a = byKey.computeIfAbsent(key, k -> new Agg());
            a.bids++;
            if (b.getBidPrice() != null) a.bidPriceSum += b.getBidPrice();
            if (winReqs.contains(b.getRequestId())) a.wins++;
        }
        List<TargetingResponse.TargetingBucket> out = new ArrayList<>();
        for (Map.Entry<String, Agg> e : byKey.entrySet()) {
            Agg a = e.getValue();
            double winRate = a.bids > 0 ? (double) a.wins / a.bids : 0.0;
            double avgBid = a.bids > 0 ? a.bidPriceSum / a.bids : 0.0;
            out.add(new TargetingResponse.TargetingBucket(e.getKey(), a.bids, a.wins, winRate, avgBid));
        }
        out.sort(Comparator.comparingLong(TargetingResponse.TargetingBucket::bids).reversed());
        return out;
    }

    // ── Timeseries ───────────────────────────────────────────────────────────────

    public Mono<TimeseriesResponse> getTimeseries(int windowMinutes, int bucketSeconds) {
        int clampedWindow = Math.max(1, Math.min(180, windowMinutes));
        int clampedBucket = Math.max(10, bucketSeconds);
        LocalDateTime since = LocalDateTime.now().minusMinutes(clampedWindow);

        Mono<List<BidRecord>> bidsMono = bidRecordRepository.findByCreatedAtAfter(since).collectList();
        Mono<List<WinNotice>> winsMono = winNoticeRepository.findByReceivedAtAfter(since).collectList();

        return Mono.zip(bidsMono, winsMono).map(t -> {
            List<BidRecord> records = t.getT1();
            List<WinNotice> wins = t.getT2();
            long bucketMillis = clampedBucket * 1000L;

            // Bucket key = epoch-millis floored to the bucket boundary.
            Map<Long, TsAgg> buckets = new java.util.TreeMap<>();
            for (BidRecord r : records) {
                long ms = r.getCreatedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                long b = ms - (ms % bucketMillis);
                TsAgg agg = buckets.computeIfAbsent(b, k -> new TsAgg());
                agg.auctions++;
                if (r.getBidPrice() != null) {
                    agg.bids++;
                    agg.bidPriceSum += r.getBidPrice();
                }
            }
            for (WinNotice w : wins) {
                long ms = w.getReceivedAt().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
                long b = ms - (ms % bucketMillis);
                TsAgg agg = buckets.computeIfAbsent(b, k -> new TsAgg());
                agg.wins++;
                agg.spend += w.getClearingPrice();
            }

            List<TimeseriesResponse.Point> points = new ArrayList<>();
            for (Map.Entry<Long, TsAgg> e : buckets.entrySet()) {
                TsAgg a = e.getValue();
                LocalDateTime time = java.time.Instant.ofEpochMilli(e.getKey())
                        .atZone(java.time.ZoneId.systemDefault()).toLocalDateTime();
                double bidRate = a.auctions > 0 ? (double) a.bids / a.auctions : 0.0;
                double winRate = a.bids > 0 ? (double) a.wins / a.bids : 0.0;
                double avgBid = a.bids > 0 ? a.bidPriceSum / a.bids : 0.0;
                points.add(new TimeseriesResponse.Point(
                        time, a.auctions, a.bids, a.wins, bidRate, winRate, avgBid, a.spend));
            }
            return new TimeseriesResponse(properties.getId(), clampedWindow, clampedBucket, points);
        });
    }

    private static final class TsAgg {
        long auctions;
        long bids;
        long wins;
        double bidPriceSum;
        double spend;
    }
}

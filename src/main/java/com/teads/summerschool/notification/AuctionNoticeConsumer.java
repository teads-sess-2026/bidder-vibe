package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidRecordRepository;
import com.teads.summerschool.record.BidderStatsCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final BidRecordRepository bidRecordRepository;
    private final WinNoticeRepository winNoticeRepository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;

    public AuctionNoticeConsumer(BidRecordRepository bidRecordRepository,
                                 WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics) {
        this.bidRecordRepository = bidRecordRepository;
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    @Transactional
    public void consume(byte[] message) {
        try {
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);
            boolean won = properties.getId().equals(notice.getWinningBidderId());

            log.debug("KAFKA  id={} winner={} won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                // TODO: handle win — record the win, decrement creative budget, update metrics
                // Hints:
                //   - Resolve creativeId from bidRecordRepository.findByRequestId(notice.getRequestId())
                //   - Call statsCache.recordWin(creativeId, notice.getClearingPrice())
                //   - Call metrics.recordWin(notice.getClearingPrice())
                //   - Save a WinNotice via winNoticeRepository
                log.info("** WIN  id={} clearing={} — not yet handled", notice.getRequestId(), notice.getClearingPrice());
            } else {
                // TODO: handle loss — update metrics if we bid on this auction
                // Hints:
                //   - Look up the bid record with bidRecordRepository.findByRequestId(notice.getRequestId())
                //   - If our bidPrice != null, call metrics.recordLoss()
            }
        } catch (Exception e) {
            log.error("** KAFKA ERROR  failed to process auction notice: {}", e.getMessage());
        }
    }
}

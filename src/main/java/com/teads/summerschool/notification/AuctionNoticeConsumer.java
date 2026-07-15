package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AuctionNoticeConsumer {

    private static final Logger log = LoggerFactory.getLogger(AuctionNoticeConsumer.class);

    private final WinNoticeRepository winNoticeRepository;
    private final BidderProperties properties;
    private final BidderStatsCache statsCache;
    private final BidderMetrics metrics;
    private final OwnBidCache ownBidCache;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        // TODO: parse the auction notice and handle wins/losses
    }
}

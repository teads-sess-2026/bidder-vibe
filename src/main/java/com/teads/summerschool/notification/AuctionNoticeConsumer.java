package com.teads.summerschool.notification;

import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.actuate.logging.LoggersEndpoint;
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
    private final LoggersEndpoint loggersEndpoint;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache, LoggersEndpoint loggersEndpoint) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
        this.loggersEndpoint = loggersEndpoint;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        // TODO: parse the auction notice and handle wins/losses
        try {
            // Parse the notice
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);
            /*
              Every auction's outcome to every bidder, so most messager
              a bidder receives are ones it never bids on.
              Filter on the in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
              Postgres - an O(1) local lookup isntead of a DB round trip on every message
             */

            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());

            if(ourBid == null) {
                log.info("Our bid is null");
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());
            log.info("KAFKA id={}, winner={}, won={}", notice.getRequestId(),notice.getWinningBidderId(), won);

            if(won){
              /*
              TODO: handle win - record the win, decrement creative budget, update metrics
              Hints:
              OurBid.creativeId() / ourBid.bidPrice() is what we bid on the auction
              Call statsCache.recordWin(ourBid.creativeId()), notice.getClearingPrice())
              returns a Mono<Double> and winNoticeRepository.save(...) returns a Mono<WinNotice> safe to block() here,
              this listener runs on its own dedicated Kafka consumer thread, not the Netty event loop
              Call metrics.recordWin(notice.getClearingPrice())
              Save a WinNotice via winNoticeRepository.save(...)
               */

            } else {

            }

        }catch (Exception e){
            log.error("Error while consuming message", e);
        }
    }
}

package com.teads.summerschool.notification;

import com.teads.summerschool.bidding.BiddingService;
import com.teads.summerschool.config.BidderProperties;
import com.teads.summerschool.metrics.BidderMetrics;
import com.teads.summerschool.proto.AuctionNoticeProto;
import com.teads.summerschool.record.BidderStatsCache;
import com.teads.summerschool.record.OwnBidCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
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
    private final BiddingService biddingService;

    public AuctionNoticeConsumer(WinNoticeRepository winNoticeRepository,
                                 BidderProperties properties,
                                 BidderStatsCache statsCache,
                                 BidderMetrics metrics,
                                 OwnBidCache ownBidCache,
                                 @Lazy BiddingService biddingService) {
        this.winNoticeRepository = winNoticeRepository;
        this.properties = properties;
        this.statsCache = statsCache;
        this.metrics = metrics;
        this.ownBidCache = ownBidCache;
        this.biddingService = biddingService;
    }

    @KafkaListener(topics = "${kafka.topic.auction-notifications}",
            autoStartup = "${spring.kafka.listener.auto-startup:true}")
    public void consume(byte[] message) {
        try {
            // Parse the notice
            AuctionNoticeProto.AuctionNotice notice = AuctionNoticeProto.AuctionNotice.parseFrom(message);
            /*
              The SSP broadcasts every auction's outcome to every bidder, so most messages
              a bidder receives are ones it never bids on.
              Filter on the in-memory OwnBidCache (see BiddingService.bid()) BEFORE touching Redis or
              Postgres - an O(1) local lookup instead of a DB round trip on every message.
             */

            OwnBidCache.Entry ourBid = ownBidCache.get(notice.getRequestId());

            if (ourBid == null) {
                // We never bid on this auction (or it aged out of the cache) — nothing to do.
                return;
            }

            boolean won = properties.getId().equals(notice.getWinningBidderId());
            log.info("KAFKA id={}, winner={}, won={}", notice.getRequestId(), notice.getWinningBidderId(), won);

            if (won) {
                // Record the win: update the market-learning stats, bump metrics, and persist the
                // win. The budget key is NOT decremented here — the SSP owns spend and has already
                // decremented it atomically. Blocking is safe here — this listener runs on its own
                // dedicated Kafka consumer thread, not the Netty event loop.
                double clearingPrice = notice.getClearingPrice();
                metrics.recordWin(clearingPrice);
                // Feed live cleared spend into the bidder's linear pacing tracker.
                biddingService.recordSpend(clearingPrice);
                winNoticeRepository.save(new WinNotice(
                        notice.getRequestId(),
                        properties.getId(),
                        clearingPrice,
                        ourBid.bidPrice())).block();
            } else {
                // We bid but lost. Track it so win-rate is observable in metrics, and feed the
                // clearing price into the market window — this is exactly what it would have cost
                // to win, the signal the bidder was previously blind to (we only ever saw prices
                // at/below what we already won). Blocking is safe on this dedicated consumer thread.
                metrics.recordLoss();
                statsCache.recordLoss(notice.getClearingPrice()).block();
            }

        } catch (Exception e) {
            log.error("Error while consuming message", e);
        }
    }
}

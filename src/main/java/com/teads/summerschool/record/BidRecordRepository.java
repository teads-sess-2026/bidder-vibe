package com.teads.summerschool.record;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public interface BidRecordRepository extends JpaRepository<BidRecord, Long> {

    Optional<BidRecord> findByRequestId(String requestId);

    long countByBidPriceIsNotNull();

    List<BidRecord> findByBidPriceIsNotNull();

    List<BidRecord> findByCreatedAtAfter(LocalDateTime since);

    List<BidRecord> findAllByRequestIdIn(Collection<String> requestIds);

    @Query("SELECT b.noBidReason, COUNT(b) FROM BidRecord b WHERE b.noBidReason IS NOT NULL GROUP BY b.noBidReason")
    List<Object[]> countGroupByNoBidReason();

    @Query("SELECT COALESCE(AVG(b.bidPrice), 0) FROM BidRecord b WHERE b.bidPrice IS NOT NULL")
    double avgBidPrice();

    @Query("SELECT b.latencyMs FROM BidRecord b WHERE b.latencyMs IS NOT NULL ORDER BY b.latencyMs")
    List<Integer> findAllLatenciesSorted();

    @Query("SELECT MIN(b.createdAt) FROM BidRecord b")
    Optional<LocalDateTime> findFirstCreatedAt();

}

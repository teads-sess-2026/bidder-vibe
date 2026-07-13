package com.teads.summerschool.record;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface BidRecordRepository extends JpaRepository<BidRecord, Long> {

    Optional<BidRecord> findByRequestId(String requestId);

}

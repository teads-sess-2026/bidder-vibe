package com.teads.summerschool.notification;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

public interface WinNoticeRepository extends JpaRepository<WinNotice, Long> {

    @Query("SELECT COALESCE(SUM(w.clearingPrice), 0) FROM WinNotice w")
    double sumClearingPrice();

    @Query("SELECT AVG(w.clearingPrice) FROM WinNotice w")
    Optional<Double> avgClearingPrice();

    List<WinNotice> findByReceivedAtAfter(LocalDateTime since);
}

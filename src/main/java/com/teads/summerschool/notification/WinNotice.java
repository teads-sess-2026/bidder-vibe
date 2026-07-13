package com.teads.summerschool.notification;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
public class WinNotice {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String requestId;

    @Column(nullable = false)
    private String bidderId;

    @Column(nullable = false)
    private double clearingPrice;

    private double bidPrice;

    @Column(nullable = false)
    private LocalDateTime receivedAt = LocalDateTime.now();

    public WinNotice() {}

    public WinNotice(String requestId, String bidderId, double clearingPrice, double bidPrice) {
        this.requestId = requestId;
        this.bidderId = bidderId;
        this.clearingPrice = clearingPrice;
        this.bidPrice = bidPrice;
    }

    public Long getId() { return id; }

    public String getRequestId() { return requestId; }
    public String getBidderId() { return bidderId; }
    public double getClearingPrice() { return clearingPrice; }
    public double getBidPrice() { return bidPrice; }
    public LocalDateTime getReceivedAt() { return receivedAt; }
}

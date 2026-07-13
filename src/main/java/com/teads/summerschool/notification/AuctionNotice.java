package com.teads.summerschool.notification;

public record AuctionNotice(
        String requestId,
        double clearingPrice,
        String winningBidderId
) {}

package com.teads.summerschool.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "bidder")
public class BidderProperties {

    private String id = "teads-bidder";
    // Flat budget assigned to each creative on seed; remaining is tracked per creative in Redis.
    private double creativeBudget = 25.0;
    private long timeoutMs = 1000;
    private Strategy strategy = new Strategy();
    private Competition competition = new Competition();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public double getCreativeBudget() { return creativeBudget; }
    public void setCreativeBudget(double creativeBudget) { this.creativeBudget = creativeBudget; }

    public long getTimeoutMs() { return timeoutMs; }
    public void setTimeoutMs(long timeoutMs) { this.timeoutMs = timeoutMs; }

    public Strategy getStrategy() { return strategy; }
    public void setStrategy(Strategy strategy) { this.strategy = strategy; }

    public Competition getCompetition() { return competition; }
    public void setCompetition(Competition competition) { this.competition = competition; }

    public static class Strategy {
        private int minSamples = 10;
        private double coldStartMultiplier = 1.25;
        private int windowSize = 50;
        private double marketMultiplier = 1.05;
        //private double premiumMultiplier = 1.5;
        private double pacingBoost = 1.20;
        private double pacingCut = 0.85;
        // How strongly the pacing multiplier reacts to the gap between the back-loaded target
        // spend and the actual budget-spent fraction. multiplier = 1 + sensitivity * gap, clamped
        // to [cut, boost]. The gap is at most ±1, so a sensitivity of 1.0 reaches the full range.
        private double pacingSensitivity = 1.0;

       //Made a curve to spend more at the end when the overall fill drops
        private double pacingCurveExponent = 1.5;

        private double throttleReleaseFraction = 0.80;

        private double throttleSensitivity = 3.0;

        private double throttleMaxSkip = 0.95;

        // Minimum-to-win pricing: bid this multiple of the rolling clearing price — just enough
        // to win the cheaper-than-average auctions without overpaying. Also gates auction entry:
        // skip auctions whose floor already exceeds this estimated minimum-to-win price.
        private double winMarginMultiplier = 1.05;

        // Floor on per-auction entry probability when behind the spend pace (see shouldBid).
        private double minEnterProbability = 0.25;

        // Keep a creative eligible while it can still afford the current auction, instead of
        // benching it once it drops below a flat $5 — avoids stranding budget at window end.
        private double minCreativeBudget = 0.5;

        public int getMinSamples() { return minSamples; }
        public void setMinSamples(int minSamples) { this.minSamples = minSamples; }

        public double getColdStartMultiplier() { return coldStartMultiplier; }
        public void setColdStartMultiplier(double coldStartMultiplier) { this.coldStartMultiplier = coldStartMultiplier; }

        public int getWindowSize() { return windowSize; }
        public void setWindowSize(int windowSize) { this.windowSize = windowSize; }

        public double getMarketMultiplier() { return marketMultiplier; }
        public void setMarketMultiplier(double marketMultiplier) { this.marketMultiplier = marketMultiplier; }

        //public double getPremiumMultiplier() { return premiumMultiplier; }
        //public void setPremiumMultiplier(double premiumMultiplier) { this.premiumMultiplier = premiumMultiplier; }

        public double getPacingBoost() { return pacingBoost; }
        public void setPacingBoost(double pacingBoost) { this.pacingBoost = pacingBoost; }

        public double getPacingCut() { return pacingCut; }
        public void setPacingCut(double pacingCut) { this.pacingCut = pacingCut; }

        public double getPacingSensitivity() { return pacingSensitivity; }
        public void setPacingSensitivity(double pacingSensitivity) { this.pacingSensitivity = pacingSensitivity; }

        public double getPacingCurveExponent() { return pacingCurveExponent; }
        public void setPacingCurveExponent(double pacingCurveExponent) { this.pacingCurveExponent = pacingCurveExponent; }

        public double getThrottleReleaseFraction() { return throttleReleaseFraction; }
        public void setThrottleReleaseFraction(double throttleReleaseFraction) { this.throttleReleaseFraction = throttleReleaseFraction; }

        public double getThrottleSensitivity() { return throttleSensitivity; }
        public void setThrottleSensitivity(double throttleSensitivity) { this.throttleSensitivity = throttleSensitivity; }

        public double getThrottleMaxSkip() { return throttleMaxSkip; }
        public void setThrottleMaxSkip(double throttleMaxSkip) { this.throttleMaxSkip = throttleMaxSkip; }

        public double getWinMarginMultiplier() { return winMarginMultiplier; }
        public void setWinMarginMultiplier(double winMarginMultiplier) { this.winMarginMultiplier = winMarginMultiplier; }

        public double getMinEnterProbability() { return minEnterProbability; }
        public void setMinEnterProbability(double minEnterProbability) { this.minEnterProbability = minEnterProbability; }

        public double getMinCreativeBudget() { return minCreativeBudget; }
        public void setMinCreativeBudget(double minCreativeBudget) { this.minCreativeBudget = minCreativeBudget; }
    }

    public static class Competition {
        // ISO-8601 instant, e.g. "2026-06-01T09:00:00Z". Empty = pacing disabled.
        private String startTime = "";
        private long durationSeconds = 1800;

        public String getStartTime() { return startTime; }
        public void setStartTime(String startTime) { this.startTime = startTime; }

        public long getDurationSeconds() { return durationSeconds; }
        public void setDurationSeconds(long durationSeconds) { this.durationSeconds = durationSeconds; }
    }
}

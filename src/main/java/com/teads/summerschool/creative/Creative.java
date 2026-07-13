package com.teads.summerschool.creative;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "creatives")
public class Creative {

    @Id
    private String id;
    private String name;
    private String description;
    private String imageUrl;
    private String callToAction;
    private String bidderId;

    // Per-creative budget limit (competition currency). Initial value seeded from
    // bidder.creative-budget; remaining budget is tracked live in Redis.
    private double budget;

    // Comma-separated values; empty string means no restriction
    @Column(length = 512)
    private String allowedGeos = "";
    @Column(length = 512)
    private String allowedDevices = "";
    @Column(length = 512)
    private String audienceSegments = "";

    public Creative() {}

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    public String getImageUrl() { return imageUrl; }
    public void setImageUrl(String imageUrl) { this.imageUrl = imageUrl; }

    public String getCallToAction() { return callToAction; }
    public void setCallToAction(String callToAction) { this.callToAction = callToAction; }

    public String getBidderId() { return bidderId; }
    public void setBidderId(String bidderId) { this.bidderId = bidderId; }

    public double getBudget() { return budget; }
    public void setBudget(double budget) { this.budget = budget; }

    public String getAllowedGeos() { return allowedGeos; }
    public void setAllowedGeos(String allowedGeos) { this.allowedGeos = allowedGeos; }

    public String getAllowedDevices() { return allowedDevices; }
    public void setAllowedDevices(String allowedDevices) { this.allowedDevices = allowedDevices; }

    public String getAudienceSegments() { return audienceSegments; }
    public void setAudienceSegments(String audienceSegments) { this.audienceSegments = audienceSegments; }

    public boolean matches(String geo, String deviceType, String audienceSegment) {
        return matchesField(allowedGeos, geo)
                && matchesField(allowedDevices, deviceType)
                && matchesField(audienceSegments, audienceSegment);
    }

    private boolean matchesField(String allowed, String value) {
        if (allowed == null || allowed.isBlank()) return true;
        for (String entry : allowed.split(",")) {
            if (entry.trim().equalsIgnoreCase(value)) return true;
        }
        return false;
    }
}

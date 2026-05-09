package com.stockdice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity(name = "company_profile")
public class CompanyProfile {

    @Id
    private String symbol;
    private String companyName;
    private Long marketCap;
    private String currency;
    private Boolean isEtf;
    private Boolean isFund;
    private Long lastUpdatedUs;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getCompanyName() {
        return companyName;
    }

    public void setCompanyName(String companyName) {
        this.companyName = companyName;
    }

    public Long getMarketCap() {
        return marketCap;
    }

    public void setMarketCap(Long marketCap) {
        this.marketCap = marketCap;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public Boolean getIsEtf() {
        return isEtf;
    }

    public void setIsEtf(Boolean isEtf) {
        this.isEtf = isEtf;
    }

    public Boolean getIsFund() {
        return isFund;
    }

    public void setIsFund(Boolean isFund) {
        this.isFund = isFund;
    }

    public Long getLastUpdatedUs() {
        return lastUpdatedUs;
    }

    public void setLastUpdatedUs(Long lastUpdatedUs) {
        this.lastUpdatedUs = lastUpdatedUs;
    }
}

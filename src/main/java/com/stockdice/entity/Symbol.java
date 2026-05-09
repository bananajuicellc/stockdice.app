package com.stockdice.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;

@Entity(name = "symbol")
public class Symbol {

    @Id
    private String symbol;

    @Column(name = "company_name")
    private String companyName;

    @Column(name = "trading_currency")
    private String tradingCurrency;

    @Column(name = "reporting_currency")
    private String reportingCurrency;

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

    public String getTradingCurrency() {
        return tradingCurrency;
    }

    public void setTradingCurrency(String tradingCurrency) {
        this.tradingCurrency = tradingCurrency;
    }

    public String getReportingCurrency() {
        return reportingCurrency;
    }

    public void setReportingCurrency(String reportingCurrency) {
        this.reportingCurrency = reportingCurrency;
    }

    public Long getLastUpdatedUs() {
        return lastUpdatedUs;
    }

    public void setLastUpdatedUs(Long lastUpdatedUs) {
        this.lastUpdatedUs = lastUpdatedUs;
    }
}

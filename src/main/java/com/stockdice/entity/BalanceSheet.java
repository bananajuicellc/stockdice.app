package com.stockdice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;

@Entity(name = "balance_sheet")
@IdClass(BalanceSheetId.class)
public class BalanceSheet {

    @Id
    private String symbol;
    @Id
    private Integer fiscalYear;
    @Id
    private String period;

    private Long totalAssets;
    private Long totalLiabilities;
    private Long lastUpdatedUs;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public Integer getFiscalYear() {
        return fiscalYear;
    }

    public void setFiscalYear(Integer fiscalYear) {
        this.fiscalYear = fiscalYear;
    }

    public String getPeriod() {
        return period;
    }

    public void setPeriod(String period) {
        this.period = period;
    }

    public Long getTotalAssets() {
        return totalAssets;
    }

    public void setTotalAssets(Long totalAssets) {
        this.totalAssets = totalAssets;
    }

    public Long getTotalLiabilities() {
        return totalLiabilities;
    }

    public void setTotalLiabilities(Long totalLiabilities) {
        this.totalLiabilities = totalLiabilities;
    }

    public Long getLastUpdatedUs() {
        return lastUpdatedUs;
    }

    public void setLastUpdatedUs(Long lastUpdatedUs) {
        this.lastUpdatedUs = lastUpdatedUs;
    }
}

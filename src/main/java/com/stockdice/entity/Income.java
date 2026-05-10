package com.stockdice.entity;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;

@Entity(name = "income")
@IdClass(IncomeId.class)
public class Income {

    @Id
    private String symbol;
    @Id
    private Integer fiscalYear;
    @Id
    private String period;

    private Long revenue;
    private Long netIncome;
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

    public Long getRevenue() {
        return revenue;
    }

    public void setRevenue(Long revenue) {
        this.revenue = revenue;
    }

    public Long getNetIncome() {
        return netIncome;
    }

    public void setNetIncome(Long netIncome) {
        this.netIncome = netIncome;
    }

    public Long getLastUpdatedUs() {
        return lastUpdatedUs;
    }

    public void setLastUpdatedUs(Long lastUpdatedUs) {
        this.lastUpdatedUs = lastUpdatedUs;
    }
}

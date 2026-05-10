package com.stockdice.entity;

import java.io.Serializable;
import java.util.Objects;

public class BalanceSheetId implements Serializable {
    private String symbol;
    private Integer fiscalYear;
    private String period;

    public BalanceSheetId() {}

    public BalanceSheetId(String symbol, Integer fiscalYear, String period) {
        this.symbol = symbol;
        this.fiscalYear = fiscalYear;
        this.period = period;
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BalanceSheetId that = (BalanceSheetId) o;
        return Objects.equals(symbol, that.symbol) &&
                Objects.equals(fiscalYear, that.fiscalYear) &&
                Objects.equals(period, that.period);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, fiscalYear, period);
    }
}

package com.stockdice.service;

public class DiceResult {
    private String symbol;
    private String companyName;
    private Double marketCapUsd;

    public DiceResult(String symbol, String companyName, Double marketCapUsd) {
        this.symbol = symbol;
        this.companyName = companyName;
        this.marketCapUsd = marketCapUsd;
    }

    public String getSymbol() {
        return symbol;
    }

    public String getCompanyName() {
        return companyName;
    }

    public Double getMarketCapUsd() {
        return marketCapUsd;
    }
}

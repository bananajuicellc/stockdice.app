package com.stockdice.service;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DataRefreshService {

    private final SymbolDownloadService symbolDownloadService;
    private final ForexDownloadService forexDownloadService;
    private final CompanyProfileDownloadService companyProfileDownloadService;
    private final BalanceSheetDownloadService balanceSheetDownloadService;
    private final IncomeDownloadService incomeDownloadService;

    public DataRefreshService(SymbolDownloadService symbolDownloadService,
                              ForexDownloadService forexDownloadService,
                              CompanyProfileDownloadService companyProfileDownloadService,
                              BalanceSheetDownloadService balanceSheetDownloadService,
                              IncomeDownloadService incomeDownloadService) {
        this.symbolDownloadService = symbolDownloadService;
        this.forexDownloadService = forexDownloadService;
        this.companyProfileDownloadService = companyProfileDownloadService;
        this.balanceSheetDownloadService = balanceSheetDownloadService;
        this.incomeDownloadService = incomeDownloadService;
    }

    // Run every hour to continuously refresh data as the python script did
    @Scheduled(fixedDelay = 3600000)
    public void downloadMarketData() {
        symbolDownloadService.downloadSymbolList();

        List<String> forexSymbols = forexDownloadService.downloadForexList();
        for (String forexSymbol : forexSymbols) {
            forexDownloadService.downloadForexQuote(forexSymbol);
        }

        List<String> companySymbols = symbolDownloadService.listSymbols();
        for (String symbol : companySymbols) {
            companyProfileDownloadService.downloadCompanyProfile(symbol);
            balanceSheetDownloadService.downloadBalanceSheet(symbol);
            incomeDownloadService.downloadIncome(symbol);
        }
    }
}

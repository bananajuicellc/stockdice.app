package com.stockdice.service;

import com.stockdice.client.FmpClient;
import com.stockdice.config.FmpConfig;
import com.stockdice.entity.BalanceSheet;
import com.stockdice.repository.BalanceSheetRepository;
import com.stockdice.repository.CompanyProfileRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class BalanceSheetDownloadService {

    private final BalanceSheetRepository balanceSheetRepository;
    private final CompanyProfileRepository companyProfileRepository;
    private final FmpClient fmpClient;
    private final FmpConfig fmpConfig;

    private static final String FMP_BALANCE_SHEET = "https://financialmodelingprep.com/stable/balance-sheet-statement?symbol={symbol}&apikey={apikey}";
    private static final long MAX_AGE_OUTSIDE_TRADING_HOURS_US = 24L * 60L * 60L * 1000000L;

    public BalanceSheetDownloadService(BalanceSheetRepository balanceSheetRepository,
                                       CompanyProfileRepository companyProfileRepository,
                                       FmpClient fmpClient, FmpConfig fmpConfig) {
        this.balanceSheetRepository = balanceSheetRepository;
        this.companyProfileRepository = companyProfileRepository;
        this.fmpClient = fmpClient;
        this.fmpConfig = fmpConfig;
    }

    public static class BalanceSheetDto {
        public String symbol;
        public Integer fiscalYear;
        public String period;
        public Long totalAssets;
        public Long totalLiabilities;
    }

    public void downloadBalanceSheet(String symbol) {
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return;
        }

        long nowUs = System.currentTimeMillis() * 1000;
        Optional<BalanceSheet> mostRecent = balanceSheetRepository.findTopBySymbolOrderByLastUpdatedUsDesc(symbol);
        if (mostRecent.isPresent() && mostRecent.get().getLastUpdatedUs() != null) {
            if (nowUs - mostRecent.get().getLastUpdatedUs() <= MAX_AGE_OUTSIDE_TRADING_HOURS_US) {
                return; // Data is fresh
            }
        }

        try {
            ResponseEntity<List<BalanceSheetDto>> response = fmpClient.get(
                    FMP_BALANCE_SHEET,
                    new ParameterizedTypeReference<List<BalanceSheetDto>>() {},
                    symbol,
                    fmpConfig.getApiKey()
            );

            if (response.getBody() != null) {
                if (response.getBody().isEmpty()) {
                    BalanceSheet bs = new BalanceSheet();
                    bs.setSymbol(symbol);
                    bs.setFiscalYear(0);
                    bs.setPeriod("EMPTY");
                    bs.setLastUpdatedUs(nowUs);
                    balanceSheetRepository.save(bs);
                } else {
                    for (BalanceSheetDto dto : response.getBody()) {
                        BalanceSheet bs = new BalanceSheet();
                        bs.setSymbol(dto.symbol != null ? dto.symbol : symbol);
                        bs.setFiscalYear(dto.fiscalYear);
                        bs.setPeriod(dto.period);
                        bs.setTotalAssets(dto.totalAssets);
                        bs.setTotalLiabilities(dto.totalLiabilities);
                        bs.setLastUpdatedUs(nowUs);
                        balanceSheetRepository.save(bs);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

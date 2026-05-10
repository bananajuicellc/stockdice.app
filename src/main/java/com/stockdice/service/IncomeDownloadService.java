package com.stockdice.service;

import com.stockdice.client.FmpClient;
import com.stockdice.config.FmpConfig;
import com.stockdice.entity.Income;
import com.stockdice.repository.IncomeRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class IncomeDownloadService {

    private final IncomeRepository incomeRepository;
    private final FmpClient fmpClient;
    private final FmpConfig fmpConfig;

    private static final String FMP_INCOME = "https://financialmodelingprep.com/stable/income-statement?symbol={symbol}&apikey={apikey}";
    private static final long MAX_AGE_OUTSIDE_TRADING_HOURS_US = 24L * 60L * 60L * 1000000L;

    public IncomeDownloadService(IncomeRepository incomeRepository, FmpClient fmpClient, FmpConfig fmpConfig) {
        this.incomeRepository = incomeRepository;
        this.fmpClient = fmpClient;
        this.fmpConfig = fmpConfig;
    }

    public static class IncomeDto {
        public String symbol;
        public Integer fiscalYear;
        public String period;
        public Long revenue;
        public Long netIncome;
    }

    public void downloadIncome(String symbol) {
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return;
        }

        long nowUs = System.currentTimeMillis() * 1000;
        Optional<Income> mostRecent = incomeRepository.findTopBySymbolOrderByLastUpdatedUsDesc(symbol);
        if (mostRecent.isPresent() && mostRecent.get().getLastUpdatedUs() != null) {
            if (nowUs - mostRecent.get().getLastUpdatedUs() <= MAX_AGE_OUTSIDE_TRADING_HOURS_US) {
                return; // Data is fresh
            }
        }

        try {
            ResponseEntity<List<IncomeDto>> response = fmpClient.get(
                    FMP_INCOME,
                    new ParameterizedTypeReference<List<IncomeDto>>() {},
                    symbol,
                    fmpConfig.getApiKey()
            );

            if (response.getBody() != null) {
                if (response.getBody().isEmpty()) {
                    Income income = new Income();
                    income.setSymbol(symbol);
                    income.setFiscalYear(0);
                    income.setPeriod("EMPTY");
                    income.setLastUpdatedUs(nowUs);
                    incomeRepository.save(income);
                } else {
                    for (IncomeDto dto : response.getBody()) {
                        Income income = new Income();
                        income.setSymbol(dto.symbol != null ? dto.symbol : symbol);
                        income.setFiscalYear(dto.fiscalYear);
                        income.setPeriod(dto.period);
                        income.setRevenue(dto.revenue);
                        income.setNetIncome(dto.netIncome);
                        income.setLastUpdatedUs(nowUs);
                        incomeRepository.save(income);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

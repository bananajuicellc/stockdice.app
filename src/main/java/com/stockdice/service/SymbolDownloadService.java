package com.stockdice.service;

import com.stockdice.client.FmpClient;
import com.stockdice.config.FmpConfig;
import com.stockdice.entity.Symbol;
import com.stockdice.repository.SymbolRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class SymbolDownloadService {

    private final SymbolRepository symbolRepository;
    private final FmpClient fmpClient;
    private final FmpConfig fmpConfig;

    private static final String FMP_FINANCIAL_STATEMENT_SYMBOL_LIST = "https://financialmodelingprep.com/stable/financial-statement-symbol-list?apikey={apikey}";

    public SymbolDownloadService(SymbolRepository symbolRepository, FmpClient fmpClient, FmpConfig fmpConfig) {
        this.symbolRepository = symbolRepository;
        this.fmpClient = fmpClient;
        this.fmpConfig = fmpConfig;
    }

    public static class SymbolDto {
        public String symbol;
        public String companyName;
        public String tradingCurrency;
        public String reportingCurrency;
    }

    public void downloadSymbolList() {
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return;
        }

        try {
            ResponseEntity<List<SymbolDto>> response = fmpClient.get(
                    FMP_FINANCIAL_STATEMENT_SYMBOL_LIST,
                    new ParameterizedTypeReference<List<SymbolDto>>() {},
                    fmpConfig.getApiKey()
            );

            long nowUs = System.currentTimeMillis() * 1000;

            if (response.getBody() != null) {
                List<Symbol> symbols = response.getBody().stream().map(dto -> {
                    Symbol s = symbolRepository.findById(dto.symbol).orElse(new Symbol());
                    s.setSymbol(dto.symbol);
                    s.setCompanyName(dto.companyName);
                    s.setTradingCurrency(dto.tradingCurrency);
                    s.setReportingCurrency(dto.reportingCurrency);
                    s.setLastUpdatedUs(nowUs);
                    return s;
                }).toList();

                symbolRepository.saveAll(symbols);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> listSymbols() {
        return symbolRepository.findByTradingCurrency("USD").stream().map(Symbol::getSymbol).toList();
    }
}

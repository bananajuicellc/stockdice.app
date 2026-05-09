package com.stockdice.service;

import com.stockdice.client.FmpClient;
import com.stockdice.config.FmpConfig;
import com.stockdice.entity.Forex;
import com.stockdice.repository.ForexRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Service
public class ForexDownloadService {

    private final ForexRepository forexRepository;
    private final FmpClient fmpClient;
    private final FmpConfig fmpConfig;

    private static final String FMP_FOREX_LIST = "https://financialmodelingprep.com/stable/forex-list?apikey={apikey}";
    private static final String FMP_FOREX_QUOTE = "https://financialmodelingprep.com/stable/quote?symbol={symbol}&apikey={apikey}";
    private static final long MAX_AGE_US = 60L * 60L * 1000000L; // 60 minutes in microseconds

    public ForexDownloadService(ForexRepository forexRepository, FmpClient fmpClient, FmpConfig fmpConfig) {
        this.forexRepository = forexRepository;
        this.fmpClient = fmpClient;
        this.fmpConfig = fmpConfig;
    }

    public static class ForexListDto {
        public String symbol;
        public String fromCurrency;
        public String toCurrency;
    }

    public static class ForexQuoteDto {
        public Double price;
    }

    public List<String> downloadForexList() {
        List<String> validSymbols = new ArrayList<>();
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return validSymbols;
        }

        try {
            ResponseEntity<List<ForexListDto>> response = fmpClient.get(
                    FMP_FOREX_LIST,
                    new ParameterizedTypeReference<List<ForexListDto>>() {},
                    fmpConfig.getApiKey()
            );

            if (response.getBody() != null) {
                for (ForexListDto dto : response.getBody()) {
                    if (dto.toCurrency == null || !dto.toCurrency.equalsIgnoreCase("USD")) {
                        continue;
                    }

                    Forex forex = forexRepository.findById(dto.symbol).orElse(new Forex());
                    forex.setSymbol(dto.symbol);
                    forex.setFromCurrency(dto.fromCurrency);
                    forex.setToCurrency(dto.toCurrency);
                    forexRepository.save(forex);

                    validSymbols.add(dto.symbol);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return validSymbols;
    }

    public void downloadForexQuote(String symbol) {
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return;
        }

        long nowUs = System.currentTimeMillis() * 1000;
        Optional<Forex> optionalForex = forexRepository.findById(symbol);
        if (optionalForex.isPresent() && optionalForex.get().getLastUpdatedUs() != null) {
            if (nowUs - optionalForex.get().getLastUpdatedUs() <= MAX_AGE_US) {
                return; // Data is fresh
            }
        }

        try {
            ResponseEntity<List<ForexQuoteDto>> response = fmpClient.get(
                    FMP_FOREX_QUOTE,
                    new ParameterizedTypeReference<List<ForexQuoteDto>>() {},
                    symbol,
                    fmpConfig.getApiKey()
            );

            if (response.getBody() != null && !response.getBody().isEmpty()) {
                ForexQuoteDto dto = response.getBody().get(0);
                if (dto.price != null) {
                    if (optionalForex.isPresent()) {
                        Forex forex = optionalForex.get();
                        forex.setPrice(dto.price);
                        forex.setLastUpdatedUs(nowUs);
                        forexRepository.save(forex);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

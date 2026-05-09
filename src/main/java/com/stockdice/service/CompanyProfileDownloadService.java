package com.stockdice.service;

import com.stockdice.client.FmpClient;
import com.stockdice.config.FmpConfig;
import com.stockdice.entity.CompanyProfile;
import com.stockdice.repository.CompanyProfileRepository;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class CompanyProfileDownloadService {

    private final CompanyProfileRepository companyProfileRepository;
    private final FmpClient fmpClient;
    private final FmpConfig fmpConfig;

    private static final String FMP_COMPANY_PROFILE = "https://financialmodelingprep.com/stable/profile?symbol={symbol}&apikey={apikey}";
    private static final long MAX_AGE_US = 60L * 60L * 1000000L; // 60 minutes in microseconds

    public CompanyProfileDownloadService(CompanyProfileRepository companyProfileRepository, FmpClient fmpClient, FmpConfig fmpConfig) {
        this.companyProfileRepository = companyProfileRepository;
        this.fmpClient = fmpClient;
        this.fmpConfig = fmpConfig;
    }

    public static class ProfileDto {
        public String symbol;
        public String companyName;
        public Long marketCap;
        public String currency;
        public Boolean isEtf;
        public Boolean isFund;
    }

    public void downloadCompanyProfile(String symbol) {
        if (fmpConfig.getApiKey() == null || fmpConfig.getApiKey().isEmpty()) {
            return;
        }

        long nowUs = System.currentTimeMillis() * 1000;
        Optional<CompanyProfile> existingProfile = companyProfileRepository.findById(symbol);
        if (existingProfile.isPresent() && existingProfile.get().getLastUpdatedUs() != null) {
            if (nowUs - existingProfile.get().getLastUpdatedUs() <= MAX_AGE_US) {
                return; // Data is fresh
            }
        }

        try {
            ResponseEntity<List<ProfileDto>> response = fmpClient.get(
                    FMP_COMPANY_PROFILE,
                    new ParameterizedTypeReference<List<ProfileDto>>() {},
                    symbol,
                    fmpConfig.getApiKey()
            );

            if (response.getBody() != null && !response.getBody().isEmpty()) {
                ProfileDto dto = response.getBody().get(0);
                CompanyProfile profile = existingProfile.orElse(new CompanyProfile());
                profile.setSymbol(dto.symbol);
                profile.setCompanyName(dto.companyName);
                profile.setMarketCap(dto.marketCap);
                profile.setCurrency(dto.currency);
                profile.setIsEtf(dto.isEtf);
                profile.setIsFund(dto.isFund);
                profile.setLastUpdatedUs(nowUs);

                companyProfileRepository.save(profile);
            } else if (response.getBody() != null && response.getBody().isEmpty()) {
                // Empty but successful response
                CompanyProfile profile = existingProfile.orElse(new CompanyProfile());
                profile.setSymbol(symbol);
                profile.setLastUpdatedUs(nowUs);
                companyProfileRepository.save(profile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

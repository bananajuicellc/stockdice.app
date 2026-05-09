package com.stockdice.service;

import com.stockdice.entity.CompanyProfile;
import com.stockdice.entity.Forex;
import com.stockdice.repository.CompanyProfileRepository;
import com.stockdice.repository.ForexRepository;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class DiceService {

    private final CompanyProfileRepository companyProfileRepository;
    private final ForexRepository forexRepository;
    private final Random random;

    public DiceService(CompanyProfileRepository companyProfileRepository, ForexRepository forexRepository) {
        this.companyProfileRepository = companyProfileRepository;
        this.forexRepository = forexRepository;
        this.random = new Random();
    }

    private List<DiceResult> getValidProfiles() {
        List<CompanyProfile> profiles = companyProfileRepository.findByIsEtfFalseAndIsFundFalse();
        List<Forex> forexList = forexRepository.findByToCurrency("USD");

        Map<String, Double> forexMap = new HashMap<>();
        for (Forex forex : forexList) {
            forexMap.put(forex.getFromCurrency(), forex.getPrice());
        }

        List<DiceResult> validProfiles = new ArrayList<>();
        for (CompanyProfile profile : profiles) {
            if (profile.getMarketCap() == null || profile.getCurrency() == null) {
                continue;
            }
            Double priceForex = forexMap.get(profile.getCurrency());
            if (priceForex != null) {
                double marketCapUsd = profile.getMarketCap() * priceForex;
                if (marketCapUsd > 0) {
                    validProfiles.add(new DiceResult(profile.getSymbol(), profile.getCompanyName(), marketCapUsd));
                }
            }
        }
        return validProfiles;
    }

    public DiceResult rollUniform() {
        List<DiceResult> validProfiles = getValidProfiles();
        if (validProfiles.isEmpty()) {
            return null; // Or throw an exception
        }
        return validProfiles.get(random.nextInt(validProfiles.size()));
    }

    public DiceResult rollMarketCap() {
        List<DiceResult> validProfiles = getValidProfiles();
        if (validProfiles.isEmpty()) {
            return null; // Or throw an exception
        }

        double totalMarketCap = 0;
        for (DiceResult profile : validProfiles) {
            totalMarketCap += profile.getMarketCapUsd();
        }

        double target = random.nextDouble() * totalMarketCap;
        double cumulativeSum = 0;

        for (DiceResult profile : validProfiles) {
            cumulativeSum += profile.getMarketCapUsd();
            if (cumulativeSum >= target) {
                return profile;
            }
        }

        return validProfiles.get(validProfiles.size() - 1);
    }
}

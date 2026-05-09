package com.stockdice.client;

import com.stockdice.config.FmpConfig;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class FmpClient {

    private final RestTemplate restTemplate;
    private final FmpConfig fmpConfig;
    private static final long SECONDS_BETWEEN_REQUESTS_MS = 200; // 5 req/sec (300/min)
    private long nextRequestTimeMs = System.currentTimeMillis();

    public FmpClient(RestTemplate restTemplate, FmpConfig fmpConfig) {
        this.restTemplate = restTemplate;
        this.fmpConfig = fmpConfig;
    }

    public synchronized <T> ResponseEntity<T> get(String url, ParameterizedTypeReference<T> responseType, Object... uriVariables) {
        long currentTimeMs = System.currentTimeMillis();
        if (currentTimeMs < nextRequestTimeMs) {
            try {
                Thread.sleep(nextRequestTimeMs - currentTimeMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while rate limiting", e);
            }
        }
        nextRequestTimeMs = System.currentTimeMillis() + SECONDS_BETWEEN_REQUESTS_MS;

        return restTemplate.exchange(url, HttpMethod.GET, null, responseType, uriVariables);
    }
}

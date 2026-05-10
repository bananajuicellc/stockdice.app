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
    private static final long NANOS_BETWEEN_REQUESTS = 200_000_000L; // 200 ms in nanoseconds
    private long nextRequestTimeNanos = System.nanoTime();

    public FmpClient(RestTemplate restTemplate, FmpConfig fmpConfig) {
        this.restTemplate = restTemplate;
        this.fmpConfig = fmpConfig;
    }

    public synchronized <T> ResponseEntity<T> get(String url, ParameterizedTypeReference<T> responseType, Object... uriVariables) {
        long currentNanos = System.nanoTime();
        if (currentNanos - nextRequestTimeNanos < 0) { // Check if current time is before the deadline
            try {
                long sleepMs = (nextRequestTimeNanos - currentNanos) / 1_000_000L;
                int sleepNanos = (int) ((nextRequestTimeNanos - currentNanos) % 1_000_000L);
                Thread.sleep(sleepMs, sleepNanos);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while rate limiting", e);
            }
        }
        nextRequestTimeNanos = System.nanoTime() + NANOS_BETWEEN_REQUESTS;

        return restTemplate.exchange(url, HttpMethod.GET, null, responseType, uriVariables);
    }
}

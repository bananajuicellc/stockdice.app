package com.stockdice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class StockDiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(StockDiceApplication.class, args);
    }

}

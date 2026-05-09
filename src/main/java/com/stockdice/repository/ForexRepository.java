package com.stockdice.repository;

import com.stockdice.entity.Forex;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface ForexRepository extends JpaRepository<Forex, String> {
    List<Forex> findByToCurrency(String toCurrency);
}

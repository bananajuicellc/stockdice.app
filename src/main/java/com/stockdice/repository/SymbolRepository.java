package com.stockdice.repository;

import com.stockdice.entity.Symbol;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface SymbolRepository extends JpaRepository<Symbol, String> {
    List<Symbol> findByTradingCurrency(String currency);
}

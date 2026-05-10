package com.stockdice.repository;

import com.stockdice.entity.BalanceSheet;
import com.stockdice.entity.BalanceSheetId;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface BalanceSheetRepository extends JpaRepository<BalanceSheet, BalanceSheetId> {
    Optional<BalanceSheet> findTopBySymbolOrderByLastUpdatedUsDesc(String symbol);
}

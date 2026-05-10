package com.stockdice.repository;

import com.stockdice.entity.Income;
import com.stockdice.entity.IncomeId;
import org.springframework.data.jpa.repository.JpaRepository;
import java.util.Optional;

public interface IncomeRepository extends JpaRepository<Income, IncomeId> {
    Optional<Income> findTopBySymbolOrderByLastUpdatedUsDesc(String symbol);
}

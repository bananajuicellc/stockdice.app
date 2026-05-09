package com.stockdice.repository;

import com.stockdice.entity.CompanyProfile;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CompanyProfileRepository extends JpaRepository<CompanyProfile, String> {
    List<CompanyProfile> findByIsEtfFalseAndIsFundFalse();
}

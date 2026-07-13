package com.teads.summerschool.creative;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface CreativeRepository extends JpaRepository<Creative, String> {

    List<Creative> findByBidderId(String bidderId);
}

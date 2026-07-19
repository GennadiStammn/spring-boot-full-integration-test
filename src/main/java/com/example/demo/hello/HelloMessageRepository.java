package com.example.demo.hello;

import org.springframework.data.jpa.repository.JpaRepository;

public interface HelloMessageRepository extends JpaRepository<HelloMessage, Long> {
}

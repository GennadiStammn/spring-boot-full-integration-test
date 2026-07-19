package com.example.demo.hello;

import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

@Entity
@Table(name = "hello")
public class HelloMessage {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String hello;

	protected HelloMessage() {
	}

	public HelloMessage(String hello) {
		this.hello = hello;
	}

	public Long getId() {
		return id;
	}

	public String getHello() {
		return hello;
	}
}

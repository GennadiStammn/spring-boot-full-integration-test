package com.example.demo.hello;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloController {

	private final HelloMessageRepository helloMessageRepository;
	private final KafkaTemplate<String, String> kafkaTemplate;

	public HelloController(HelloMessageRepository helloMessageRepository, KafkaTemplate<String, String> kafkaTemplate) {
		this.helloMessageRepository = helloMessageRepository;
		this.kafkaTemplate = kafkaTemplate;
	}

	@PostMapping(path = "/hello", consumes = "text/plain")
	public ResponseEntity<Long> createHello(@RequestBody String hello) {
		HelloMessage savedHello = helloMessageRepository.save(new HelloMessage(hello));
		kafkaTemplate.send("hello", hello);
		return ResponseEntity.status(HttpStatus.CREATED).body(savedHello.getId());
	}
}

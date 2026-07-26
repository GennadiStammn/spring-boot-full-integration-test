package com.example.demo.hello.kafka;

import com.example.demo.hello.HelloMessage;
import com.example.demo.hello.HelloMessageRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class HelloMessageListener {

	private final HelloMessageRepository helloMessageRepository;

	public HelloMessageListener(HelloMessageRepository helloMessageRepository) {
		this.helloMessageRepository = helloMessageRepository;
	}

	@KafkaListener(topics = "hello-avro", groupId = "hello-message-consumer")
	public void receive(com.example.demo.avro.HelloMessage message) {
		helloMessageRepository.save(new HelloMessage(message.getFreeText().toString()));
	}
}

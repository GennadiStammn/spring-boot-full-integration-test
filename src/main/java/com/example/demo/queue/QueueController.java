package com.example.demo.queue;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.ResponseEntity;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class QueueController {

	private final MessageChannel queueChannel;

	public QueueController(@Qualifier("queueChannel") MessageChannel queueChannel) {
		this.queueChannel = queueChannel;
	}

	@PostMapping(path = "/queue", consumes = "text/plain")
	public ResponseEntity<Void> queue(@RequestBody String text) {
		queueChannel.send(MessageBuilder.withPayload(text).build());
		return ResponseEntity.accepted().build();
	}
}

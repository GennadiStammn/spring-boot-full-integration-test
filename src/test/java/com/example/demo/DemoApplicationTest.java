package com.example.demo;

import com.example.demo.avro.HelloMessage;
import com.example.demo.hello.HelloMessageRepository;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class DemoApplicationTest extends BaseIntegrationTest {

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private HelloMessageRepository helloMessageRepository;

	@Test
	void when_post_hello_should_store_message_in_database() throws Exception {
		var token = keycloakContainer.getAccessToken("demo", "demo-client", "test@test.com", "test");
		String hello = "Hello from integration test";

		MvcResult result = mockMvc.perform(post("/hello")
						.header("Authorization", "Bearer " + token)
						.contentType(TEXT_PLAIN)
						.content(hello))
				.andExpect(status().isCreated())
				.andReturn();

		Long id = Long.valueOf(result.getResponse().getContentAsString());
		assertEquals(hello, helloMessageRepository.findById(id).orElseThrow().getHello());
		assertTrue(readHelloTopic().contains(hello));
	}

	@Test
	void when_receive_avro_hello_should_store_message_in_database() throws Exception {
		String hello = "Hello from Avro";
		Map<String, Object> producerProperties = Map.of(
				ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
				ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
				ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
				"schema.registry.url", "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());

		try (KafkaProducer<String, HelloMessage> producer =
					 new KafkaProducer<>(producerProperties)) {
			producer.send(new ProducerRecord<>("hello-avro", new HelloMessage(hello))).get();
		}

		Instant timeout = Instant.now().plusSeconds(10);
		while (Instant.now().isBefore(timeout)
				&& helloMessageRepository.findAll().stream().noneMatch(message -> hello.equals(message.getHello()))) {
			Thread.sleep(100);
		}

		assertTrue(helloMessageRepository.findAll().stream()
				.anyMatch(message -> hello.equals(message.getHello())));
	}

	private List<String> readHelloTopic() {
		Map<String, Object> consumerProperties = Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
				ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString(),
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
			consumer.subscribe(List.of("hello"));
			Instant timeout = Instant.now().plusSeconds(10);
			List<String> values = new java.util.ArrayList<>();
			while (Instant.now().isBefore(timeout) && values.isEmpty()) {
				for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
					values.add(record.value());
				}
			}
			return values;
		}
	}
}

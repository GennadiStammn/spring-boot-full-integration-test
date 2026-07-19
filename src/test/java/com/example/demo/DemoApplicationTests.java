package com.example.demo;

import com.example.demo.hello.HelloMessageRepository;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.http.MediaType.TEXT_PLAIN;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
class DemoApplicationTests {

	@Container
	@ServiceConnection
	public static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:15")
			.withDatabaseName("postgres")
			.withUsername("postgres")
			.withPassword("postgres");

	@Container
	public static KeycloakContainer keycloakContainer = new KeycloakContainer("quay.io/keycloak/keycloak:latest")
                    .withEnv("KEYCLOAK_ADMIN", "admin")
                    .withEnv("KEYCLOAK_ADMIN_PASSWORD", "password")
                    .withRealmImportFile("realm/demo-realm.json");

	@Container
	public static ConfluentKafkaContainer kafka = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0")
			.withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private HelloMessageRepository helloMessageRepository;

	@Test
	void postHelloStoresTextInDatabase() throws Exception {
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

	@DynamicPropertySource
	static void registerResourceServerIssuerProperty(DynamicPropertyRegistry registry) {
		registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> keycloakContainer.getAuthServerUrl() + "/realms/demo");
		registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
	}

	private List<String> readHelloTopic() {
		Map<String, Object> consumerProperties = Map.of(
				ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
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

package com.example.demo;

import com.example.demo.hello.HelloMessageRepository;
import dasniko.testcontainers.keycloak.KeycloakContainer;
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
	}

	@DynamicPropertySource
	static void registerResourceServerIssuerProperty(DynamicPropertyRegistry registry) {
		registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> keycloakContainer.getAuthServerUrl() + "/realms/demo");
	}
}

package com.example.demo;

import com.example.demo.hello.HelloMessageRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
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
	public static PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:11.1")
			.withDatabaseName("postgres")
			.withUsername("postgres")
			.withPassword("postgres");

	@Autowired
	private MockMvc mockMvc;

	@Autowired
	private HelloMessageRepository helloMessageRepository;

	@Test
	void contextLoads() {

	}

	@Test
	void postHelloStoresTextInDatabase() throws Exception {
		String hello = "Hello from integration test";

		MvcResult result = mockMvc.perform(post("/hello")
						.contentType(TEXT_PLAIN)
						.content(hello))
				.andExpect(status().isCreated())
				.andReturn();

		Long id = Long.valueOf(result.getResponse().getContentAsString());
		assertEquals(hello, helloMessageRepository.findById(id).orElseThrow().getHello());
	}

}

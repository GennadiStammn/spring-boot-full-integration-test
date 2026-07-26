package com.example.demo;

import dasniko.testcontainers.keycloak.KeycloakContainer;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public abstract class BaseIntegrationTest {

    protected static final Network NETWORK = Network.newNetwork();

    @Container
    @ServiceConnection
    protected static final PostgreSQLContainer postgreSQLContainer = new PostgreSQLContainer("postgres:15")
            .withNetwork(NETWORK)
            .withDatabaseName("postgres")
            .withUsername("postgres")
            .withPassword("postgres");


    @Container
    @ServiceConnection
    protected static final ConfluentKafkaContainer kafkaContainer = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.4.0")
            .withNetwork(NETWORK)
            .dependsOn(postgreSQLContainer)
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withListener("kafka:19092");

    @Container
    protected static final GenericContainer<?> schemaRegistry =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.5.2"))
                    .withNetwork(NETWORK)
                    .withExposedPorts(8081)
                    .dependsOn(kafkaContainer)
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                            "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    @Container
    protected static final KeycloakContainer keycloakContainer = new KeycloakContainer("quay.io/keycloak/keycloak:latest")
            .withNetwork(NETWORK)
            .dependsOn(schemaRegistry)
            .withEnv("KEYCLOAK_ADMIN", "admin")
            .withEnv("KEYCLOAK_ADMIN_PASSWORD", "password")
            .withRealmImportFile("realm/demo-realm.json");


    @DynamicPropertySource
    static void registerResourceServerIssuerProperty(DynamicPropertyRegistry registry) {
        registry.add("spring.security.oauth2.resourceserver.jwt.issuer-uri", () -> keycloakContainer.getAuthServerUrl() + "/realms/demo");
        registry.add("spring.kafka.schema.registry.url", () -> "http://" + schemaRegistry.getHost() + ":" + schemaRegistry.getFirstMappedPort());
    }
}

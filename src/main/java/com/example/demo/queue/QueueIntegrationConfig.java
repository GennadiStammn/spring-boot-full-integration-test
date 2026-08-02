package com.example.demo.queue;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.jdbc.store.JdbcChannelMessageStore;
import org.springframework.integration.jdbc.store.channel.PostgresChannelMessageStoreQueryProvider;
import org.springframework.integration.store.MessageGroupQueue;
import org.springframework.messaging.MessageHandler;

@Configuration
public class QueueIntegrationConfig {

	private static final String QUEUE_GROUP_ID = "queued-text";

	@Bean
	JdbcChannelMessageStore jdbcChannelMessageStore(DataSource dataSource) {
		JdbcChannelMessageStore messageStore = new JdbcChannelMessageStore(dataSource);
		messageStore.setChannelMessageStoreQueryProvider(new PostgresChannelMessageStoreQueryProvider());
		return messageStore;
	}

	@Bean
	QueueChannel queueChannel(JdbcChannelMessageStore jdbcChannelMessageStore) {
		return new QueueChannel(new MessageGroupQueue(jdbcChannelMessageStore, QUEUE_GROUP_ID));
	}

	@Bean
	@ServiceActivator(inputChannel = "queueChannel", poller = @Poller(fixedDelay = "2000"))
	MessageHandler queueMessageHandler() {
		return message -> System.out.println(message.getPayload());
	}
}

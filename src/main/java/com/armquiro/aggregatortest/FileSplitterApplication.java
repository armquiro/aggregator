package com.armquiro.aggregatortest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.integration.aggregator.DefaultAggregatingMessageGroupProcessor;
import org.springframework.integration.aggregator.MessageGroupProcessor;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.expression.FunctionExpression;
import org.springframework.integration.file.dsl.Files;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.ChainFileListFilter;
import org.springframework.integration.file.filters.ExpressionFileListFilter;
import org.springframework.integration.jdbc.store.JdbcMessageStore;
import org.springframework.integration.store.MessageGroupStore;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalTime;
import java.time.ZoneId;

@SpringBootApplication
public class FileSplitterApplication {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	public static void main(String[] args) {
		SpringApplication.run(FileSplitterApplication.class, args);
	}

	@Bean
	public IntegrationFlow fileSplitterFlow() {
		URL url1 = this.getClass().getResource("/temps");

		return IntegrationFlow
				.from(Files.inboundAdapter(new File(url1.getFile()))
						.filter(new ChainFileListFilter<File>()
								.addFilter(new AcceptOnceFileListFilter<>())
								.addFilter(new ExpressionFileListFilter<>(
										new FunctionExpression<File>(f -> "foo.tmp".equals(f.getName()))
								))))
				.split(Files.splitter()
						.markers()
						.charset(StandardCharsets.US_ASCII)
						.applySequence(true)
				)
				.channel(c -> c.queue("fileSplittingResultChannel"))
				.get();
	}

	@Bean
	public MessageGroupStore messageGroupStore() {
		return new JdbcMessageStore(jdbcTemplate);
	}

	@Bean
	public MessageGroupProcessor messageGroupProcessor() {
		return new DefaultAggregatingMessageGroupProcessor();
	}

	@Bean
	public CorrelationStrategy correlationStrategy() {
    return message -> {
      // Define your correlation logic here
      // This example uses a constant correlation key
			//System.out.println("Message: " + message.getPayload().toString());
			return message.getPayload().toString().charAt(0);
    };
	}

	public long getGroupTimeout(){
			// Get the current time
			LocalTime currentTimeLocal = LocalTime.now(ZoneId.of("America/New_York"));
			// Define the target time (8 PM)
			LocalTime targetTime = LocalTime.of(20, 0); // 8 PM is represented as 20:00 in 24-hour format
			// Calculate the duration between current time and 8 PM
			Duration durationUntil8PM = Duration.between(currentTimeLocal, targetTime);
			// Convert the duration to milliseconds
			long millisecondsUntil8PMLocal = durationUntil8PM.toMillis();
			// Print the result
			System.out.println("Milliseconds until 6 PM: " + millisecondsUntil8PMLocal);
			return millisecondsUntil8PMLocal;
	}

	@Bean
	public IntegrationFlow aggregatorFlow(MessageGroupStore messageGroupStore, ExceptionAwareReleaseStrategy releaseStrategy, MyCorrelationStrategy correlationStrategy) {
		return IntegrationFlow.from("fileSplittingResultChannel")
				.filter(message -> {
					if(message.getClass() == String.class) System.out.println(">>> " + message.toString());
					return message.getClass() == String.class;
				})
				.aggregate(a -> a
						.messageStore(messageGroupStore)
						.correlationStrategy(correlationStrategy)
						.releaseStrategy(releaseStrategy)
						.expireGroupsUponCompletion(false)
						.groupTimeout(getGroupTimeout())
						.expireGroupsUponTimeout(false)
						.sendPartialResultOnExpiry(true)
				)
				.handle(new MyMessageHandler(releaseStrategy, correlationStrategy))
				.get();
	}
}



package com.armquiro.aggregatortest;

import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.integration.util.UUIDConverter;
import org.springframework.messaging.Message;
import org.springframework.integration.aggregator.ReleaseStrategy;
import org.springframework.messaging.MessageHandler;

import java.util.UUID;

public class MyMessageHandler implements MessageHandler {

  private final ReleaseStrategy releaseStrategy;

  private final CorrelationStrategy correlationStrategy;


  public MyMessageHandler(ReleaseStrategy releaseStrategy, CorrelationStrategy correlationStrategy) {
    this.releaseStrategy = releaseStrategy;
    this.correlationStrategy = correlationStrategy;
  }

  @Override
  public void handleMessage(Message<?> message) {
    try {
      // Process the message here
      // If an exception occurs, catch it and mark the group with an exception
      System.out.println("---------- Aggregated Message: ");
      System.out.println("payload " + message.getPayload());
      System.out.println("headers " + message.getHeaders());

      if (message.getPayload().toString().contains("3t")) {
        throw new RuntimeException("Ha Fallado!!");
      }

    } catch (Exception ex) {
      // Handle the exception
      ex.printStackTrace();

      Object correlationKey = this.correlationStrategy.getCorrelationKey(message);

      UUID groupIdUuid = UUIDConverter.getUUID(correlationKey);

      System.out.println("Handling message with correlationKey [" + correlationKey + "]: " + message);
      System.out.println("Handling message with correlationKey [" + groupIdUuid + "]: ");

      // Mark the group with an exception in the release strategy
      ((ExceptionAwareReleaseStrategy)releaseStrategy).markGroupWithException(groupIdUuid);

    }
  }
}


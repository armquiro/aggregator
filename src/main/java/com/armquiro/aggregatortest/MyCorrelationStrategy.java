package com.armquiro.aggregatortest;

import org.springframework.integration.aggregator.CorrelationStrategy;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

@Component
public class MyCorrelationStrategy implements CorrelationStrategy {

  // Correlation strategy groups messages using the first character in payload.
  @Override
  public Object getCorrelationKey(Message<?> message) {
    return message.getPayload().toString().charAt(0);
  }
}

package com.confluentproducer.server.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.ssl.DefaultSslBundleRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
@Getter
@Slf4j
public class KafkaProducerConfig {

//  private final ObjectMapper objectMapper;

  @Value("${sample.kafka.producer.topics.sampleTopic}")
  private String sampleTopic;

//  KafkaProducerConfig(ObjectMapper objectMapper) {
//    this.objectMapper = objectMapper;
//  }
//
  @Bean
  public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> producerFactory) {
    return new KafkaTemplate<>(producerFactory);
  }

  @Bean
  public ProducerFactory<String, Object> producerFactory(KafkaProperties kafkaProperties) {
    Map<String, Object> producerProps = new HashMap<>(
        kafkaProperties.getProducer().buildProperties(new DefaultSslBundleRegistry()));
    log.info("Kafka Producer properties: {}", producerProps);
    return new DefaultKafkaProducerFactory<>(producerProps);
  }
}

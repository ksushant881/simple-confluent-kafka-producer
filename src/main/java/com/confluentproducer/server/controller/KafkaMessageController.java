package com.confluentproducer.server.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Component
@RequestMapping("/api/kafka/test")
@Slf4j
@RequiredArgsConstructor
public class KafkaMessageController {

  private static final String TOPIC_PREFIX = "dev.v5.";
  private static final String DOMAIN = "sample-domain";
  private final ObjectMapper objectMapper;

  private final KafkaTemplate<String, Object> kafkaTemplate;

  @PostMapping("/send/{eventType}")
  public ResponseEntity<String> sendMessage(@PathVariable String eventType,
      @RequestBody String payload) {
    try {
      String eventId = UUID.randomUUID().toString();
      String eventTime = OffsetDateTime.now().toString();
      String topic = TOPIC_PREFIX + eventType + ".notifications";

      Object typedPayload = parsePayload(eventType, payload, eventId);
      ProducerRecord<String, Object> producerRecord = new ProducerRecord<>(topic,
          UUID.randomUUID().toString(), typedPayload);

      addHeaders(producerRecord, eventId, eventTime, eventType);
      kafkaTemplate.send(producerRecord);
      kafkaTemplate.flush();

      return ResponseEntity.ok()
          .body(String.format("Message sent with eventId: %s, eventType: %s", eventId, eventType));

    } catch (Exception ex) {
      log.error("Failed to send message", ex);
      return ResponseEntity.badRequest().body("Failed to send message: " + ex.getMessage());
    }
  }

  private static final Map<String, String> EVENT_TYPE_MAPPING = new HashMap<>();

  static {
    EVENT_TYPE_MAPPING.put("type1", "TYPE-1");
    EVENT_TYPE_MAPPING.put("type2", "TYPE-2");
  }

  private void addHeaders(ProducerRecord<String, Object> producerRecord, String eventId,
      String eventTime, String eventType) {
    producerRecord.headers()
        .add(new RecordHeader("eventId", eventId.getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers()
        .add(new RecordHeader("eventTime", eventTime.getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers().add(new RecordHeader("eventType",
        EVENT_TYPE_MAPPING.get(eventType).getBytes(StandardCharsets.UTF_8)));
    producerRecord.headers()
        .add(new RecordHeader("domain", DOMAIN.getBytes(StandardCharsets.UTF_8)));
  }

  private Object parsePayload(String eventType, String payload, String id) {
    try {
      Map<String, Object> payloadMap = objectMapper.readValue(payload, Map.class);
      payloadMap.put("id", id);

      Class<?> targetClass = switch (eventType) {
        case "type1" -> Object.class;
        case "type2" -> Number.class;
        default -> throw new IllegalArgumentException("Unknown event type: " + eventType);
      };

      return objectMapper.convertValue(payloadMap, targetClass);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Failed to parse payload for event type: " + eventType, e);
    }
  }
}

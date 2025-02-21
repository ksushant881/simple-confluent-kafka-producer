package com.confluentproducer.server.serialization;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import java.io.IOException;
import java.io.InterruptedIOException;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.header.Headers;
import org.openapitools.jackson.nullable.JsonNullableModule;

@Slf4j
@RequiredArgsConstructor
public class KafkaJsonSchemaSerializerExtension extends KafkaJsonSchemaSerializer<Object> {

  private static final JsonSchema EMPTY_SCHEMA = new JsonSchema("{}");
  private final ObjectMapper mapper;

  public KafkaJsonSchemaSerializerExtension() {
    super();
    mapper = new ObjectMapper();
    mapper.registerModule(new JavaTimeModule());
    mapper.registerModule(new JsonNullableModule());
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
  }

  @Override
  @SneakyThrows
  public byte[] serialize(String topic, Headers headers, Object data) {
    byte[] allBytes = toByteArray(data);
    JsonNode jsonNode = mapper.readTree(allBytes);
    final byte[] serializeImpl = serializeImpl(getSubjectName(topic, isKey, jsonNode, EMPTY_SCHEMA), topic, headers,
        jsonNode, EMPTY_SCHEMA);
    log.info("Outgoing {} {}", new String(headers.lastHeader("eventType").value()),
        new String(serializeImpl).substring(5));
    return serializeImpl;
  }

  @Override
  protected byte[] serializeImpl(String subject, String topic, Headers headers, Object object, JsonSchema schema)
      throws SerializationException, InvalidConfigurationException {
    if (schemaRegistry == null) {
      throw new InvalidConfigurationException("SchemaRegistryClient not found. You need to configure the serializer "
          + "or use serializer constructor with SchemaRegistryClient.");
    }
    // null needs to treated specially since the client most likely just wants to send
    // an individual null value instead of making the subject a null type. Also, null in
    // Kafka has a special meaning for deletion in a topic with the compact retention policy.
    // Therefore, we will bypass schema registration and return a null value in Kafka, instead
    // of an encoded null.
    if (object == null) {
      return new byte[0];
    }
    String restClientErrorMsg = "";
    try {
      restClientErrorMsg = "Error retrieving latest version: ";
      schema = (JsonSchema) lookupLatestVersion(subject, schema, latestCompatStrict);
      int id = schemaRegistry.getId(subject, schema);
      log.info("Got schema with id {} ", id);
      object = executeRules(subject, topic, headers, RuleMode.WRITE, null, schema, object);
      if (validate) {
        validateJson(object, schema);
      }
      return mapper.writeValueAsBytes(object);
    } catch (InterruptedIOException e) {
      throw new TimeoutException("Error serializing JSON message", e);
    } catch (IOException | RuntimeException e) {
      throw new SerializationException("Error serializing JSON message", e);
    } catch (RestClientException e) {
      throw toKafkaException(e, restClientErrorMsg + schema);
    } finally {
      postOp(object);
    }
  }

  private byte[] toByteArray(Object data) throws JsonProcessingException {
    return mapper.writeValueAsBytes(data);
  }
}


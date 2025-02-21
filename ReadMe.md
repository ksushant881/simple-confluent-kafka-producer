# Simple confluent kafka producer

- Uses a custom serializer that does not adds magic bytes while serializing
- Does schema validation
- Avoids any other thing that confluent's serializer does

### Add the following properties in application.yaml:

#### Actual confluent cloud cluster credentials
- bootstrap-servers
- sasl.jaas.config

#### Confluent cloud schema registry credentials
- schema.registry.url
- basic.auth.credentials.source (default to USER_INFO)
- basic.auth.user.info


#### Tips
- Add required headers
- Make post requests to /localhost:7094/api/kafka/{topic_name}
- Add required target class mappings 
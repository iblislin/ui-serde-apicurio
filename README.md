# ui-serde-apicurio

A pluggable [serde](https://ui.docs.kafbat.io/configuration/serialization-serde) for
[kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) that decodes **Avro whose Apicurio
schema id is carried in the Kafka record _header_** (Apicurio's `HeaderIdHandler`),
rather than in the Confluent payload magic-byte prefix.

## Why

kafka-ui's built-in Schema Registry serde resolves the schema id from the **payload**
envelope `[0x00][id 4B][avro…]` (Confluent-compat). When a producer uses Apicurio in
**header mode** (`apicurio.registry.headers.enabled=true`), the id lives in the record
headers (`apicurio.value.globalId` / `apicurio.key.globalId`) and the payload is bare
Avro — so the built-in serde fails with `Unknown magic byte!`. This plugin reads the id
from the header, fetches the writer schema from the Apicurio registry, and decodes.

No JSON library is pulled in as a choice: Avro parses the `.avsc` and renders the decoded
record to JSON itself.

## Requirements

- **kafka-ui ≥ v1.5.0** — record headers are passed to the deserializer as of
  [kafbat/kafka-ui#1768](https://github.com/kafbat/kafka-ui/pull/1768) (milestone 1.5).
- An Apicurio Registry reachable from kafka-ui, and producers writing in Apicurio
  **header mode** with the **globalId** header (the default id).

## Build

```bash
sbt assembly
# -> target/scala-3.3.8/ui-serde-apicurio-0.1.0-SNAPSHOT-assembly.jar
```

`serde-api` is `provided` (kafka-ui supplies it); the fat-jar bundles the Scala stdlib +
Avro (+ Avro's transitive Jackson).

## Install into kafka-ui

Mount the jar and register the serde (env or YAML):

```yaml
kafka:
  clusters:
    - name: my-cluster
      serde:
        - name: ApicurioHeader
          filePath: /serde/ui-serde-apicurio-0.1.0-SNAPSHOT-assembly.jar
          className: io.github.iblislin.uiserde.apicurio.ApicurioHeaderSerde
          properties:
            registryUrl: http://apicurio-host/apis/registry/v3
```

Then pick **ApicurioHeader** as the value (or key) serde when viewing a topic.

See [`docker-compose/setup-example.yaml`](docker-compose/setup-example.yaml).

## Properties

| property      | required | description                                                        |
| ------------- | -------- | ------------------------------------------------------------------ |
| `registryUrl` | yes      | Apicurio Registry v3 API base, e.g. `http://host/apis/registry/v3` |

## Scope / roadmap

- **Now:** deserialize (message viewing) only; Avro; globalId header; value + key.
- **Later:** producing in header mode (`serializer`); `contentId` header option;
  schema caching TTL; `getSchema` topic-level hints.

## Contributing

Apache-2.0. The intent is to offer this upstream to the kafbat serde ecosystem
(alongside [`ui-serde-glue`](https://github.com/kafbat/ui-serde-glue) /
`ui-serde-smile`) once it is proven in use.

Sponsored by China Medical University Hospital, Taichung, Taiwan (CMUH).

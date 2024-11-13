Connector for Apache Kafka
==========================

The `swimos_connector_kafka` crate provides both an ingress and an egress connector for Apache Kafka. Internally, it uses the [`rdkafka`](https://crates.io/crates/rdkafka) crate to communicate with the Kafka brokers. To use the connectors it is also necessary to add a dependency on the `swimos_connector` crate.

The create has feature flags `json` and `avro` to enable support for JSON (via the [`serde_json`](https://crates.io/crates/serde_json) crate) and [Apache Avro](https://avro.apache.org/) (via the [`apache-avro`](https://crates.io/crates/apache-avro) crate) as serialization formats.

Ingress connector
-----------------

### Registering the connector

To register a Kafka ingress connector with your server application, add the following:

```rust
use swimos_connector::IngressConnectorModel;
use swimos_connector_kafka::{KafkaIngressConfiguration, KafkaIngressConnector};

let connector_config: KafkaIngressConfiguration = ...;

let connector_agent = IngressConnectorModel::for_fn(move || {
    KafkaIngressConnector::for_config(connector_config.clone())
});
```

The connector can then be registered as an agent route with the `add_route` method on the server builder. For example:

```
server
    ...
    .add_route(RoutePattern::parse_str("/kafka")?)
    ...
    .build()
```

The `KafkaIngressConfiguration` can be instantiated in code or deserialized from a recon file (as it implements the `swimos_form::Form` trait):

```rust
let recon = tokio::fs::read_to_string("kafka-config.recon").await?;
KafkaIngressConfiguration::from_str(&recon)?
```

### Configuring the connector

An example configuration file for the Kafka ingress connector would be:

```recon
@kafka {
    properties: {
        "group.id": consumer-test,
        "message.timeout.ms": "5000",
        "bootstrap.servers": "localhost:9092",
        "auto.offset.reset": smallest
    },
    log_level: @Debug,
    key_deserializer: @Int32(@BigEndian),
    payload_deserializer: @Json,
    topics: {
        "example"
    }
    value_lanes: {},
    map_lanes: {},
    relays: {},
}
```

The configuration parameters are:

* `properties` - This is the collection of properties (string key value pairs) that are passed  to the underlying Kafka consumer. This must include the list of bootstrap servers that the client should connect to.
* `log_level` - This is the log level to be passed to the underlying Kafka consumer. It does not affect the logging of the Swim server application.
* `key_deserializer` - This specifies the deserializer to use for the keys of the incoming Kafka messages. This must be a variant of `swimos_connector::config::format::DataFormat`.
* `payload_deserializer` - This specifies the deserializer to use for the payloads of the incoming Kafka messages. This must be a variant of `swimos_connector::config::format::DataFormat`.
* `topics` - A list of Kafka topics for the connector to subscribe to.

The remaining fields `value_lanes`, `map_lanes` and `relays` specify how the connector should handle the incoming Kafka messages.

The specifications for each of these contain selector strings that will select components of the incoming Kafka messages. For the syntax for selectors, see [here](../selectors.md). The valid root selectors for the Kafka ingress connector are "$topic", "$key" and "$payload". The topic selector evaluates to the topic of the message (as a string) and the key and payload selectors evaluate to the values that were deserialized from these parts of the message.

#### Value lanes

Each entry in the `value_lanes` list will add a value lane to the connector agent. A value lane entry has the following format:

```recon
@ValueLaneSpec {
    name: example_name,
    selector: "$payload",
    required: true
}
```

The fields of the specification are:

1. `name` - The name of the lane. This field is optional. If it is not defined the connector will attempt infer the name from the `selector` field (in this case it would be "payload").
2. `selector` - Describes how to select a value for the value lane from each incoming Kafka message.
3. `required` - Specifies if this value should be present in every message. If it is required and the selector cannot select a value from a message, the connector will fail with an error.

#### Map lanes

Each entry in the `map_lanes` list will add a map lane to the connector agent. A map lane entry has the following format:

```recon
@MapLaneSpec {
    name: example_name,
    key_selector: "$payload.key",
    value_selector: "$payload.value",
    remove_when_no_value: false,
    required: true
}
```

For each message from the Kafka consumer, the connector will attempt to extract a pair of a key and value which it will use to update an entry in the map lane.

The fields of the specification are:

1. `name` - The name of the lane.
2. `key_selector` - Describes how to select a key for the entry.
3. `value_selector` - Describes hot to select a value for the entry.
4. `remove_when_no_value` - If this is true and the key selector returns a value while the value selector does not, the key will be removed from the map lane.
5. `required` - Specifies that an operation to be applied to the map must be selected for each Kafka message. If it is required and the selector cannot select a key an value from the message (or a key if `remote_when_no_value` is true), the connector will fail with an error.

#### Relays

For each entry in the `relays` list, each time a Kafka message is received a command will be sent to a lane on another agent. This can either be a single, fixed lane or derived from the contents of the message. Relays can point at either value-like (value lane, command lane etc) lanes or map lanes.

The format for a value relay is:

```recon
@Value @ValueRelaySpec {
    node: "/node",
    lane: lane,
    payload: "$payload",
    required: false
}
```

The format for a map relay is:

```recon
@Map @MapRelaySpec {
    node: "/node",
    lane: lane,
    key: "$key",
    value: "$payload",
    required: false,
    remove_when_no_value: true
}
```

The `node` and `lane` fields indicate which lane the command should be sent to. They can either be fixed or may contain selectors (for example `node: "/$payload.target` to choose the node based on the `target` field from the message payload).

The other fields have the same meanings as those for value lanes and map lanes above.

Egress connector
----------------

### Registering the connector

To register a Kafka egress connector with your server application, add the following:

```rust
use swimos_connector::EgressConnectorModel;
use swimos_connector_kafka::{KafkaEgressConfiguration, KafkaEgressConnector};

let connector_config: KafkaEgressConfiguration = ...;

let connector_agent = EgressConnectorModel::for_fn(move || {
    KafkaEgressConnector::for_config(connector_config.clone())
});
```

The connector can then be registered as an agent route with the `add_route` method on the server builder. For example:

```
server
    ...
    .add_route(RoutePattern::parse_str("/kafka")?)
    ...
    .build()
```

The `KafkaEgressConfiguration` can be instantiated in code or deserialized from a recon file (as it implements the `swimos_form::Form` trait):

```rust
let recon = tokio::fs::read_to_string("kafka-config.recon").await?;
KafkaEgressConfiguration::from_str(&recon)?
```

### Configuring the connector

An example configuration file for the Kafka egress connector would be:

```recon
@kafka {
    properties: {
        "message.timeout.ms": "5000",
        "group.id": producer-test,
        "bootstrap.servers": "localhost:9092",
    },
    log_level: @Debug,
    key_serializer: @Int32(@BigEndian),
    payload_serializer: @Json,
    fixed_topic: example-topic,
    retry_timeout_ms: 5000,
    value_lanes: {},
    map_lanes: {},
    event_downlinks: {},
    map_event_downlinks: {},
}
```

The configuration parameters are:

* `properties` - This is the collection of properties (string key value pairs) that are passed  to the underlying Kafka producer. This must include the list of bootstrap servers that the client should connect to.
* `log_level` - This is the log level to be passed to the underlying Kafka producer. It does not affect the logging of the Swim server application.
* `key_serializer` - This specifies the serializer to use for the keys of the outgoing Kafka messages. This must be a variant of `swimos_connector::config::format::DataFormat`.
* `payload_serializer` - This specifies the serializer to use for the payloads of the outgoing Kafka messages. This must be a variant of `swimos_connector::config::format::DataFormat`.
* `fixed_topic` - A fixed topic to send outgoing messages to. This can be overridden on a per-message basis. It is optional and if it is not defined all outgoing messages must have an explicit topic or the connector agent will fail with an error.
* `retry_timeout_ms` - If the producer is busy when the connector attempts to send a message, it will try again after this timeout period (in milliseconds).

The remaining fields `value_lanes`, `map_lanes`, `event_downlinks` and `map_event_downlinks` specify how the connector should produce outgoing Kafka messages.

The specifications for each of these contain selector strings that will select components of the events that are generated by each of the lanes and downlinks. For the syntax for selectors, see [here](../selectors.md). The valid root selectors for the Kafka egress connector are "$key", "$value". 

The key selector evaluates to the key of an event on a map lane or map downlink and will always fail to select anything for the value equivalents. The value selector will select the value associated with any event.

Each outgoing Kafka message must be sent to a specific topic. Each of the types of item listed about require a topic selector. The possible topic selectors are:

* `@Fixed` - Uses the topic give in the `fixed_topic` configuration parameter.
* `@Specified("target")` - An explicitly named topic (in this case "target").
* `@Selector("$value.topic")` - Attempts to extract the topic from the contents of the events using a selector.

#### Value and map lanes

Each entry in the `value_lanes` list will add a value lane to the connector agent. Similarly,
each entry on the `map_lanes` list will add a map lane to the agent. Both have the following format:

```recon
@LaneSpec {
    name: event,
    extractor: @ExtractionSpec {
        topic_specifier: @Fixed,
        key_selector: "$value.key",
        payload_selector: "$value.payload"
    }
}
```

The fields of the specification are:

1. `name` - The name of the lane.
2. `topic_specifier` - Describes how to select a topic from the lane events.
3. `key_selector` - Describes how to select the Kafka key from the lane events.
4. `payload_selector` - Describes how to select the Kafka payload from the lane events.

For each value set to a value lane or each key/value pair generated by an update to a map lane pair of Recon values will be extracted using the key and payload selectors. Additionally, a string value will be extracted with the topic specifier to indicate a Kafka topic. These will be combined to create a Kafka message which will be published, via the configured serializers.

The Kafka producer runs in a separate thread and, if it is too busy to accept a message, the connector will keep the most recent message for each topic and periodically retry.

#### Event and map-event downlinks

For each entry in the `event_downlinks` and `map_event_downlinks`, the connector agent will open a downlink, of the appropriate type to the specified lane. Both have the following format:

```recon
@DownlinkSpec {
    address: @Address {
        host: "localhost:9000",
        node: "/node",
        lane: "lane",
    },
    extractor: @ExtractionSpec {
        topic_specifier: @Selector("$value.topic),
        key_selector: "$value.key",
        payload_selector: "$value.payload"
    }
}
```

The `host` field indicates the SwimOS server instance where the lane is located. This is optional and if it is absent, the local instance hosting the connector will be assumed. The `node` and `lane` fields specify the coordinates of the lane.

The extractor specification works in exactly the same way as for value an map lanes.




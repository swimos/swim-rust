Connectors for External Data Sources and Sinks
==============================================

Connectors make it easier to create an interface between your Swim application and external data sources and repositories. They come in two varieties: ingress connectors and egress connectors. Ingress connectors allow you to update the the state of your application using a stream of events produced by an external system. Conversely, an egress connector observes the state of one or more Swim lanes and writes to changes into an external data sink.

For example, an ingress connector could consume a stream of messages from a queue (such as Apache Kafka) or poll a database for changes to the rows of a table. A corresponding egress connector could publish messages to the queue or write new records into the database.

SwimOS provides a number of standard connector implementations and also exposes an API for writing your own connectors. Connectors run within an SwimOS server applications as entirely normal agents. In fact, there is not reason that you could not implement your own connectors using the standard agent API. The connector API exists only as a convenience to simplify the process of writing connectors by providing a core that is applicable to many kinds of data source or sink.

Contents
--------

1. Provided connector implementations.
    * [Fluvio Connector](fluvio.md) - An ingress connector for [Fluvio](https://www.fluvio.io/).
    * [Kafka Connectors](kafka.md) - Ingress and egress connectors for [Apache Kafka](https://kafka.apache.org/).
    * [MQTT Connectors](mqtt.md) - Ingress and egress connectors for [MQTT](https://mqtt.org/) brokers.
2. The connector API.
    * TODO

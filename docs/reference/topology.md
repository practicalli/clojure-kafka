# Topology


A logical representation of a ProcessorTopology.

A topology is an acyclic graph of sources, processors, and sinks. A source is a node in the graph that consumes one or more Kafka topics and forwards them to its successor nodes.

A processor is a node in the graph that receives input records from upstream nodes, processes the records, and optionally forwarding new records to one or all of its downstream nodes.

Finally, a sink is a node in the graph that receives records from upstream nodes and writes them to a Kafka topic. A Topology allows you to construct an acyclic graph of these nodes, and then passed into a new KafkaStreams instance that will then begin consuming, processing, and producing records.

- [Topology](https://kafka.apache.org/11/javadoc/org/apache/kafka/streams/Topology.html)

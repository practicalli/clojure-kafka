# Kafka and Clojure - Immutable event streams applications

[Kafka](https://kafka.apache.org/) is Kafka is used for building real-time data pipelines and streaming apps. It is horizontally scalable, fault-tolerant, extremely fast and runs in production in thousands of companies (Braintree).

[Clojure](https://clojure.org/) is a dynamic, general-purpose programming language, combining the approachability and interactive development of a scripting language with an efficient and robust infrastructure for multithreaded programming. Clojure is a compiled language, yet remains completely dynamic – every feature supported by Clojure is supported at runtime. Clojure provides easy access to the Java frameworks, with optional type hints and type inference, to ensure that calls to Java can avoid reflection.

## Why Kafka and Clojure

- Easy interaction with and well-maintained libraries for other JVM-based systems (e.g. Kafka and ElasticSearch)
- Concurrency and IPC capabilities
- Clojure's sequence abstractions simplified the code and made testing easy

![Kafka](https://kafka.apache.org/images/kafka_diagram.png){loading=lazy style="height:420px;width:420px"}


!!! WARNING "Currently under development"
    This book is very early alpha stage and a long way from being complete


## Tutorials

* [Apache Kafka - quickstart](https://kafka.apache.org/quickstart)
* [Part 1: Apache Kafka for beginners - What is Apache Kafka - CloudKarafka](https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html)
* [Cloudurable Kafka Tutorial](http://cloudurable.com/blog/kafka-tutorial/index.html) - current tutorial for Meet-a-mentor study group
* [Apache Kafka Tutorial - TutorialsPoint](https://www.tutorialspoint.com/apache_kafka/index.htm)
* [Kafka in Clojure](https://techblog.roomkey.com/posts/clojure-kafka.html)
* [Writing a Kafka Producer and High Level Consumer in Clojure](https://wtfleming.github.io/2015/01/14/kafka-clojure-producer-consumer/)
* [Hello World Kafka](https://github.com/SubhaSingh/hello_world_kafka)


## Libraries and projects

* [franzy](https://github.com/clj-kafka/franzy) - suite of Clojure libraries for Apache Kafka. It includes libraries for Kafka consumers, producers, partitioners, callbacks, serializers, and deserializers.
* [Kafka Streams Clojure](https://github.com/bobby/kafka-streams-clojure) - Clojure transducers interface to Kafka Streams. This combo provides the best of both worlds for building streaming applications on Kafka with Clojure
* [milena](https://github.com/dvlopt/milena) - This Kafka client library allows the user to exchange records while speaking clojure
* [fundingcircle/jackdaw](https://fundingcircle.github.io/jackdaw/) - Clojure library for Apache Kafka


## Presentations on Kafka

* [Introduction to Apache Kafka by James Ward](https://www.youtube.com/watch?v=UEg40Te8pnE)
* [Building a data pipeline with Clojure and Kafka](https://speakerdeck.com/davidpick/building-a-data-pipeline-with-clojure-and-kafka)
* [Lisp in the Machine - Clojure at Braintree](https://vimeo.com/138748699)
* [Ployconf 15 - The LISP in the Machine A Clojure experience report from Braintree / Joe Nash](https://www.youtube.com/watch?v=0D3jev1E5ks)
* [One Million Clicks per Minute with Kafka and Clojure - Devon Peticolas](https://www.youtube.com/watch?v=VC_MTD68erY)
* [From REST to CQRS with Clojure, Kafka, & Datomic - Bobby Calderwood](https://www.youtube.com/watch?v=qDNPQo9UmJA&t=8s)
* [David Pick - Building a Data Pipeline with Clojure and Kafka](https://www.youtube.com/watch?v=6xlyWjqFDWs)
* [Lambda Days 2018 - Andrea Crotti - Tame Kafka streams with Clojure](https://www.youtube.com/watch?v=OC2KVaLQihs)
* ["Commander: Better Distributed Applications through CQRS and Event Sourcing" by Bobby Calderwood](https://www.youtube.com/watch?v=B1-gS0oEtYc)
* [Streaming Data Platforms & Clojure with Derek Troy-West](https://www.youtube.com/watch?v=4sUaF4m5TWI)
* [Clojure at 4,000 msg/s-What We Learned, Loved, and Loathed - Nathan Barnett](https://www.youtube.com/watch?v=zwuFJovzdHg)
* [Managing One of the World's Largest Clojure Code Bases - Donevan Dolby](https://www.youtube.com/watch?v=iUC7noGU1mQ)
* [Reducing Microservice Complexity with Kafka and Reactive Streams - by Jim Riecken](https://www.youtube.com/watch?v=k_Y5ieFHGbs&t=2s)
* [PolyConf 15: Distributed systems the easy way with Clojure and Mesos / Pierre-Yves Ritschard](https://www.youtube.com/watch?v=Tz-wwjhqEEM)

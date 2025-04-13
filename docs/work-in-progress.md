# Work in progress

Apache Kafka is popular foundation for large-scale software systems.

Learn how to use Kafka to publish and subscribe to data streams

Install and configure a Kafka cluster and how to use the Kafka API’s to produce and consume data.

How to connect Kafka to technologies for stream processing, log aggregation, and other related big-data technologies.

- Why Kafka is scalable
- How to interact with Kafka
- Kafka’s role in enterprise architectures
- How to design Kafka topics and partitions

- Install and configure Kafka
- Publish data to Kafka
- Subscribe to data from Kafka
- Design Kafka topics and partitions

ensure that systems are scalable and fault tolerant

Prerequisites

Basic knowledge of Java
A GitHub link to a description for installations will be provided.

Recommended Preparation:

Learning Modern Java: A Crash Course Using Java 8

Docker: Up & Running, Chapter 3. Installing Docker

Introduction to Apache Kafka

About your instructor
Petter Graff is the co-founder of SciSpike, helping Fortune-500 companies reach their potential through training, consulting and custom development. Petter has extensive experience building large scale software systems for many of the Fortune-500 companies. Petter was the main architect behind the open source project Yaktor (yaktor.io) which relies on Kafka to deliver messages across large clusters of computation nodes. Yaktor and Kafka has been used by various companies to build systems processing millions of messages per second. Petter is also a frequent speaker at various conferences and an O’Reilly author (check out his Video Series on Design Patterns in Java).



Schedule
The timeframes are only estimates and may vary according to how the class is progressing

DAY 1

What is Kafka
Verify that everything is installed and working (Lab ~ 20 min)

Install Kafka through Docker
Run a simple example of Kafka

## Kafka under the hood
What is a topic
What is a partition
What is a producer
What is a consumer

Creating a topic and pass a message

Create a topic
Run a simple consumer
Run a simple producer

Design of Kafka topics and partitions

Case study
How to select topics
How to select partitions
Exercise: Designing topics and partitions (Group Project ~ 20 min)
- Design topics and partitions

Evaluation of the designs and suggested solutions (Discussion ~20 min)

Discussion of the suggested solution(s)
Recommended design of case study
Implement Topics and Partitions for case study (Lab ~30 min)

Define a topic and partition in Kafka
Create a consumer and producer
Run a test script
Scaling Kafka (Lecture ~30 min)

Kafka Brokers
Kafka Clusters
Cluster mirroring
Consumer groups
Streaming APIs for Kafka (Lecture ~20 min)

What is streaming
Why use streams
Programming to streams
Example streams using Spark
Streaming and IoT Case Study (Lab ~30 min)

Consume a stream from Kafka
Build a Spark application over the Kafka stream
Kafka Administration and Integration (Lecture ~30 Min)

Integration with Big Data tools (Storm, Spark, Hadoop)
Kafka Connect
Certified Kafka connectors
Kafka administration
Kafka monitoring
Security


---

Amazing mind map about his Kafka and opening up the conversation with his findings around Zero Copying (https://www.ibm.com/developerworks/library/j-zerocopy/index.html) and backpushing

Efficient data transfer through zero copy
This article explains how you can improve the performance of I/O-intensive Java applications running on Linux and UNIX platforms through a technique called zero copy. Zero copy lets you avoid redundant data copies between intermediate buffers and reduces the number of context switches between user space and kernel space.

http://cloudurable.com/blog/kafka-architecture/index.html

Create a slack chatbot to put messaged into Kafka topics to be utilised later

Kafka Architecture
Kafka Architecture: This article discusses the structure of Kafka. Kafka consists of Records, Topics, Consumers, Producers, Brokers, Logs, Partitions, and Clusters. Records can have key, value and timestamp. Kafka Records are immutable. This article covers the structure of and purpose of topics, log, partition, segments, brokers, producers, and consumers.



Kafka Tutorial with Examples
http://cloudurable.com/blog/kafka-tutorial-v1/index.html
Cloudurable provides AWS Cassandra and Kafka support, Cassandra consulting, Cassandra training, and Kafka consulting. Our focus is on successful deployments of Cassandra and Kafka in AWS EC2. We work with the full AWS stack including Lambdas, EC2, EBS, CloudFormation, CloudWatch and more.
cloudurable.com

Kafka Architecture
http://cloudurable.com/blog/kafka-architecture/index.html
Kafka Architecture: This article discusses the structure of Kafka. Kafka consists of Records, Topics, Consumers, Producers, Brokers, Logs, Partitions, and Clusters. Records can have key, value and timestamp. Kafka Records are immutable. This article covers the structure of and purpose of topics, log, partition, segments, brokers, producers, and consumers.


http://cloudurable.com/blog/what-is-kafka/index.html



Trying to use the Kafka producer and consumer concept, and in my case, the producer is the debezium-connector and the topics are also created by it. So, I just need to use the consumer to read the messages from the topics
12:10
So, I configured my consumer via integrant like this...
```
(defmethod ig/prep-key ::consumer
  [_ {:keys [kafka-brokers kafka-group enable-auto-commit
             topics max-poll-records]
      :or {kafka-brokers "localhost:9092" kafka-group "myapp"
           enable-auto-commit false max-poll-records "100"}}]
  (timbre/info "Preparing consumer")
  {"bootstrap.servers" kafka-brokers
   "group.id" kafka-group
   "enable.auto.commit" enable-auto-commit
   "auto.offset.reset" "earliest"
   ;; Enviroment variable is always string
   "max.poll.records" (Integer/parseInt max-poll-records)
   "topics" (if topics
              (mapv #(hash-map :topic-name %) (str/split topics #","))
              (throw (IllegalArgumentException. "Kafka topics are
              required. You need specify atleast one topic.")))
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
(defmethod ig/init-key ::consumer [_ config]
  (timbre/info "Configuring Kafka consumer" config)
  (-> (jc/consumer (dissoc config :topics))
      (jc/subscribe (get config "topics"))))
(defmethod ig/halt-key! ::consumer [_ consumer]
  (timbre/info "Stopping Kafka consumer")
  (when consumer
    (.close consumer)))
```

Now any idea how do I consume the messages using this consumer...?
basically, I got stuck on how to get the topic name, like if the debezium is the one that is creating the topic and producing into it then how do I refer that to the consumer to use it...?

I can even keep an eye on the Kafka logs for all the updates also via this command
docker run -it --network=docker-debezium_default --rm edenhill/kafkacat:1.6.0 kafkacat -C -b kafka:9092 -t myapp.public.chatrooms -o -10

where myapp.public.chatrooms is the topic where all the updates are being produced
but how do I use it in the code?


jc/subscribe (and jc/consumer) both return the consumer, you then need to call jc/poll on that consumer to read the messages.
jc/poll returns just the first 'batch' of readable messages so you tend to call this in a while loop continuously:

```
(while @run
  (doseq [msg (jc/poll consumer 1000)]
    (println msg)))
 ```

The topic name format on the connector is likely to be configurable, the default is apparently server.schema.table so you should know the value in advance (edited)


Looks like jackdaw doesn't wrap it, but you can use java interop on the consumer to subscribe to a topic-name Pattern which may be more helpful here, so you could subscribe to #"myapp\.public\..*" for example to pick up all the created topics. (edited)

When using the consumer directly the pattern to go with is like this:
```
(while my-app-is-running
  (let [records (jackdaw.client/poll consumer (Duration/ofMillis 1000))]
      (doseq [{:keys [value]} records]
        ;; do something with the value
      )
      (.commitSync consumer)))
```


The other alternative is to use the Kafka Streams DSL, have a look at https://github.com/FundingCircle/jackdaw/blob/master/examples/word-count/src/word_count.clj
examples/word-count/src/word_count.clj



I got how to consume if the topic is created by me but I'm using debezium and in this case, it is the one which is creating the topic and it is the one producing the updates too... so in order for me to use that topic inside the consumer I should use the name right? but if that topic isn't present in any of the files then how to refer it?


Yeah @Daniel Stephens, the topic name is that in the connector but if it's created by the debezium connector then how do I refer to it in code?


Can you use list-topics and select the topic from there?

In the admin ns https://github.com/FundingCircle/jackdaw/blob/master/src/jackdaw/admin.clj
src/jackdaw/admin.clj


As I was using the Debezium connector, it will create topics for all tables in the database. So, I can directly refer to those.

The main reason I got confused was that I was using integrate to setup the whole thing and that kinda got messed up!


Now jackdaw.client/subscribe will just subscribe the given consumer to a specified topic and returns the consumer right? So, how to print the consumed data?


Hey @Daniel Stephens, I thought it'd be better to poll the messages and then put those messages in a core.async channel and from the streamer I can read that channel and then send a callback...
So, I tried polling them inside the mutation like this,
```
(while true
  (let [records (jc/poll consumer 1000)]
      (doseq [{:keys [value]} records]
        (println value))
      (.commitSync consumer)))
```

But I'm getting this:
class clojure.lang.PersistentArrayMap cannot be cast to class org.apache.kafka.clients.consumer.Consumer (clojure.lang.PersistentArrayMap and org.apache.kafka.clients.consumer.Consumer are in unnamed module of loader 'app'
What does this mean?

Pretty standard error in clojure, it means you are calling a function that expects a consumer on a map and it can't be cast.
It's usually pretty simple to debug these in a repl as you can divide and conquer to work out where the error is coming from.
In this case since it's a method that's expecting a consumer, as a guess it's quite likely you are calling poll or commitSync on a map by accident, I'd probably start with checking the value of consumer in your snippet

Let's say there are two topics A and B and we want to do something like this if the data is produced to topic A then do something if data is produced to topic B then do something. Is this achievable? I mean is there a way to see if a specific topic has been updated when we've multiple topics...?

or just read a message from a specific topic?

poll
(poll consumer timeout)
Polls kafka for new messages, returning a potentially empty sequence
of datafied messages.
That's for reading the messages right? But if it only takes the consumer as the argument, then how to poll from a certain topic if one consumer is subscribed to 2 topics...?

If you want to do something different for messages in each topic, why not have a different consumer for each one rather than consuming both with the same consumer?

I'm using it with graphql, so I don't know if it's feasible to have a consumer for each streamer...?
10:17
Can't I read from a specific topic?

Not if you set it up to read from many no

How do I do that? Now I'm subscribing to two topics and I just want to read from each separately...

Subscribe to just one topic and read the messages from it

Yeah, that is fine, I want to have all the chatroom-related data in the chatroom topic and all the templates related data in the template topic. So, now I'm producing data into both the topics and made the consumer subscribe to both. Now, in one function I want to read the messages from the chatroom topic, and in another, I want to read from the template topic.

What do you do with the data once you've read it?

I'm throwing it onto a core.async channel

Ok but after that? Is it going into some database?

Now, inside the graphql streamer, I'm using it. Like once the channel gets any data, it triggers the streamer thereby the streamer sends the callback in real-time...
10:27
Basically before I was using the atom, like whenever the mutation updates the data in database it throws that into the atom as well and inside the streamer I used to have an add-watch method which triggers whenever an atom changes and then it used to send a callback.
10:27
Now I have debezium producing the data to the Kafka topics, but I need something to trigger the streamer that a certain topic has been updated so that it can send the callback.
10:28
So, I thought to take the data from a topic and throw it into a channel and then from that the streamer acts...

I'm not sure exactly what a graphql streamer is. Is the scenario something like this?
 User submits a request
 Request causes update in dB
 dB changes cascade into the two Kafka topics
 Your app reads the topics and needs to respond to the user request? Or otherwise update some state on the client?

Graphql streamer shows the updates in real-time whenever any data changes in the database.

So can I do this: In one function I want to read the messages from the chatroom topic, and in another, I want to read from the template topic.


I'd probably adjust the payload you put on core.async to include the topic name. Then where you're consuming the channel, you can add a filter to exclude messages from the topic you don't want


But then I again it doesn’t work for graphql streamers na...? The chatroom streamer should trigger when the chatroom topic has some updates and the template streamer should trigger when the template topic has the updates.


But the streamer is fed by a core.async channel right?


Yeah, I mean but wait, yeah it makes sense!!!

So, basically how do I read the messages from any topic though?

with poll method?


Again, not super familiar with core.async but as I understand them, I think you can create multiple channels and link them through transducers. So one channel receives the data from Kafka (all subscribed topics), and then two channels are created and connected to the first each with a transducer that adapts the raw Kafka input as necessary for its target streamer


Got that, thank you. How do I read the data from kafka topic?

This is the method right?
poll
(poll consumer timeout)
Polls kafka for new messages, returning a potentially empty sequence
of datafied messages.


Yep


Now it takes only the consumer as the arg, so if topic 1 has produced a message then running this poll func returns what?

message from topic 1?


Yep


Same for topic 2. So, whichever the topic has got the data, the poll method returns from it/


Yep exactly

The messages returned from poll are "records" which include not only the key/value of the message but additional metadata like offset, partition, and topic so I think you can use that to augment the payload you feed to the core.async channel


So, basically, it's like consuming from a specific topic now right? As poll returns the recently updated topic data....?

This is terrific!!

Yeah in core.async I'll flter it...

I tried to poll it like this,
(while true
  (doseq [msg (jc/poll consumer 1000)]
    (println msg)))

But I'm getting this:
class clojure.lang.PersistentArrayMap cannot be cast to class org.apache.kafka.clients.consumer.Consumer (clojure.lang.PersistentArrayMap and org.apache.kafka.clients.consumer.Consumer are in unnamed module of loader 'app'


May I see the code that constructs that consumer?


```
(defmethod ig/prep-key ::consumer
  [_ {:keys [kafka-brokers kafka-group enable-auto-commit
             topics max-poll-records]
      :or {kafka-brokers "localhost:9092" kafka-group "unifize"
           enable-auto-commit false max-poll-records "100"}}]
  (timbre/info "Preparing consumer")
  {"bootstrap.servers" kafka-brokers
   "group.id" kafka-group
   "enable.auto.commit" enable-auto-commit
   "auto.offset.reset" "earliest"
   ;; Enviroment variable is always string
   "max.poll.records" (Integer/parseInt max-poll-records)
   "topics" (if topics
              (mapv #(hash-map :topic-name %) (str/split topics #","))
              (throw (IllegalArgumentException. "Kafka topics are
              required. You need specify atleast one topic.")))
   "key.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"
   "value.deserializer" "org.apache.kafka.common.serialization.StringDeserializer"})
(defmethod ig/init-key ::consumer [_ config]
  (timbre/info "Configuring Kafka consumer" config)
  (-> (jc/consumer (dissoc config :topics))
      (jc/subscribe (get config "topics"))))
(defmethod ig/halt-key! ::consumer [_ consumer]
  (timbre/info "Stopping Kafka consumer")
  (when consumer
    (.close consumer)))
```


Think it's time to read the huge stuff printed out when you printed your consumer: https://clojurians.slack.com/archives/CEA3C7UG0/p1619182766054100


```
{:request {:graphql-operation-name nil, :protocol HTTP/1.1, :async-supported? true, :com.walmartlabs.lacinia.pedestal2/timing-start {:start-time 2021-04-24T09:59:11.235602Z, :start-nanos 237837993209899}, :remote-addr 127.0.0.1, :servlet-response #object[org.eclipse.jetty.server.Response 0x4dc70389 HTTP/1.1 200
Date: Sat, 24 Apr 2021 09:59:11 GMT
], :graphql-query mutation {
    update_chatroom(uid:"L0QGp8J3P0fFNq7eGeR7Tl61UuL2", org_id:1, id:2, input:{title:"something"}){
        title
    }
},
```

Well, let's make it smaller by getting just the keys


Saying its 25k characters long and can't send it here!


Even just the keys?


No No

the entire thing

how to get only the keys?

(keys consumer)

should I print that? or ??

Yes. We're trying to debug your program. There's something which at the moment we think is a consumer object but in reality it is a map. We want to print out the keys to give us an idea of what that map is supposed to be in the hope that perhaps you can extract your consumer out of it.


I mean yeah I got that but should I do println keys consumer or ... ?


yes

Or just upload the whole "consumer" as a gist (and then provide a link) rather than copying it into slack which has the limit as you saw

If you opt for the latter, you might need to make sure there's no secrets you wouldn't want exposed to the public


I did this:
(println "This is consumer" (keys consumer))

Got this:
This is consumer
```
(:request :com.walmartlabs.lacinia.constants/parsed-query :com.walmartlabs.lacinia.constants/schema :com.walmartlabs.lacinia/container-type-name :com.walmartlabs.lacinia/selection)
```

Gist it's pasting in a single line...

is screenshot good enough?

ok, so clearly that looks more like an http request than a consumer instance.

Now...?
So, I guess, it means I'm passing map as consumer not the instance of consumer...?

I suppose back to the integrant docs to see how you're supposed to get at the services defined in integrant config

Yes that is what it means

I think there must be an ig/init in your code somewhere that sets a "system" var. I think the consumer will be in that.


I defined them like this:

```
def config
   {:com.app.graphql.kafka/should-poll? true
    :com.app.graphql.kafka/poll-timeout (env :kafka-poll-timeout)
    :com.app.graphql.kafka/consumer (select-keys env [:kafka-brokers
                                                          :kafka-group
                                                          :enable-auto-commit
                                                          :deserializer
                                                          :topics])
    :com.app.graphql/schema {:consumer (ig/ref :com.app.graphql.kafka/consumer)}
    :com.app.graphql/server {:schema (ig/ref :com.app.graphql/schema)}
 ```


and this is the server.clj
```
(defmethod ig/init-key :com.app.graphql/server [_ {schema :schema}]
  (http/start schema))
```

So by this :com.app.graphql/schema {:consumer (ig/ref :com.app.graphql.kafka/consumer)} I'm passing the consumer everywhere...

It looks fine right?


I dunno, suspect you'd get better advice about this aspect in #integrant


Okay. Thank you for your time.

No worries. Sorry I couldn't help you get over the line with your problem but I think you're close to a solution :)

Yeah just need to get the poll working...

Thank you so much. Now, I know I just need to poll it and throw it in the core.async channel...

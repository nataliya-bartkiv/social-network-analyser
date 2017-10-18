# social-network-analyser

Demo program for analysing social network activity

## Overview

Demo consists of several main parts
### Messages generation 
Random generators create multiple users, posts and actions
### Producing messages with Kafka 
* Producers push generated entities to different topics
* Topic Value is wrapped in GenericRecord (Avro Serialization)
### Analysing messages with Kafka Streaming 
* 'Streamers' consume produced messages
* Messages are joined/mapped/transformed/aggregated into some kind of statistics
* Statistics is pushed to different output topics
* Avro Serialization/Deserealization is used
### Kafka Connector to Cassandra Sink
Stats messages are consumed from appropriate topics and stored in Cassandra tables

## Helpful Info

### Starting Avro Consumer
Lets assume topic is called "users". To watch messages, produced into chosen topic, run next command:
```
sudo kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic posts
```
Bootstrap-server property is mandatory; 9092 is the default port for Kafka broker.

### Configuring connector to Cassandra

Create Cassandra Connector for post-stats topic

* Create a keyspace
```
CREATE KEYSPACE IF NOT EXISTS social_network_analyser 
WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 2 };
```
* Switch to the keyspace
```
USE social_network_analyser;
```
* Create a table
```
CREATE TABLE IF NOT EXISTS post_stats (
  postId INT PRIMARY KEY,
  likeCount INT,
  dislikeCount INT,
  repostCount INT	
);
```
* Create properties file, for example:  
```
/usr/local/stream-reactor/conf/cassandra-sink-post-stats.properties
```
It should contain next properties:
```
name=cassandra-sink-post-stats
connector.class=com.datamountaineer.streamreactor.connect.cassandra.sink.CassandraSinkConnector
tasks.max=1
topics=post-stats
connect.cassandra.kcql=INSERT INTO post_stats SELECT * FROM post-stats
connect.cassandra.port=9042
connect.cassandra.key.space=social_network_analyser
connect.cassandra.contact.points=localhost
connect.cassandra.username=cassandra
connect.cassandra.password=cassandra
```

* Configure connector
```
sudo connect-cli create cassandra-sink-post-stats \
< /usr/local/stream-reactor/conf/cassandra-sink-post-stats.properties
```
To configure Cassandra connectors for another topics, create appropriate Cassandra tables:

* For users-stats:
```
CREATE TABLE IF NOT EXISTS user_stats (
  name TEXT,
  surname TEXT,
  postCount INT,
  PRIMARY KEY(name, surname)
);
 ```
 * For country-stats:
 ``` 
CREATE TABLE IF NOT EXISTS country_stats (
  country TEXT PRIMARY KEY,
  postCount INT
);
 ```
 Repeat steps needed for post-stats connector, changing names of topic and tables.
 
 ### Useful links
 * [Kafka Connect Cassandra Sink](http://docs.datamountaineer.com/en/latest/cassandra-sink.html)
 * [Installing Stream Reactor](http://docs.datamountaineer.com/en/latest/install.html#stream-reactor-install)
 * [The Connect API in Kafka Cassandra Sink: The Perfect Match](https://www.confluent.io/blog/kafka-connect-cassandra-sink-the-perfect-match/)
 * [Kafka Streams Developer Guide](https://docs.confluent.io/current/streams/developer-guide.html#developer-guide)

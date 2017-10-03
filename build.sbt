name := "SocialNetworkAnalyser"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "Confluent Maven Repo" at "http://packages.confluent.io/maven/"

libraryDependencies ++= Seq(
    "org.apache.kafka" % "kafka_2.11" % "0.8.2.1",
    "com.github.scopt" % "scopt_2.11" % "3.7.0",
    "io.confluent" % "kafka-avro-serializer" % "3.3.0",
    "io.confluent" % "kafka-streams-avro-serde" % "3.3.0",
    "com.sksamuel.avro4s" %% "avro4s-core" % "1.6.4",
    "org.apache.httpcomponents" % "httpclient" % "4.5.3",
    "org.apache.kafka" % "kafka-streams" % "0.11.0.1",
    //"com.tuplejump" % "kafka-connect-cassandra" % "0.0.7"
)
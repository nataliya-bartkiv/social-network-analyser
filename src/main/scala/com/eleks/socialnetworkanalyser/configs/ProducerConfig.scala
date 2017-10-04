package com.eleks.socialnetworkanalyser.configs

case class ProducerConfig (
    brokerList : String = "localhost:9092",
    sync : String = "async",
    schema : String = "http://localhost:8081/",
    keySerializer: String = "io.confluent.kafka.serializers.KafkaAvroSerializer",
    valueSerializer : String = "io.confluent.kafka.serializers.KafkaAvroSerializer",
    topic : String = null
)

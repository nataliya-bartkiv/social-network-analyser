package com.eleks.socialnetworkanalyser.utilities

case class Config(brokerList : String = "localhost:9092",
                  sync : String = "async",
                  schema : String = "http://localhost:8081/",
                  keySerializer: String = "io.confluent.kafka.serializers.KafkaAvroSerializer",
                  valueSerializer : String = "io.confluent.kafka.serializers.KafkaAvroSerializer"
                 )

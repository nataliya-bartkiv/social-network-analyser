package com.eleks.socialnetworkanalyser.configs

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde

case class StreamerConfig (appId : String,
                           bootstrapServers : String,
                           keySerde : String = classOf[GenericAvroSerde].toString,
                           valueSerde : String = classOf[GenericAvroSerde].toString,
                           inputTopic : String,
                           outputTopic : String
                          )

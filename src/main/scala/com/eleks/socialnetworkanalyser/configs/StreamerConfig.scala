package com.eleks.socialnetworkanalyser.configs

import com.eleks.socialnetworkanalyser.utilities.AvroSerde

case class StreamerConfig (appId : String,
                           bootstrapServers : String = "http://localhost:9092/",
                           keySerde : String = classOf[AvroSerde].getName,
                           valueSerde : String = classOf[AvroSerde].getName,
                           inputTopic : String,
                           outputTopic : String,
                           schema : String = "http://localhost:8081/"
                          )

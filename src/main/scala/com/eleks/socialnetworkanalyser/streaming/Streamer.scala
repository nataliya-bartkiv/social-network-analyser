package com.eleks.socialnetworkanalyser.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.eleks.socialnetworkanalyser.configs.StreamerConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._

abstract class Streamer extends Runnable {
    protected var config : StreamerConfig = _
    protected var streams : KafkaStreams = _

    def configure(config : StreamerConfig) {
        this.config = config

        val properties : Properties = new Properties()
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.appId)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, config.keySerde)
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, config.keySerde)
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schema)
        properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0.toString)

        val streamBuilder = configureStreamBuilder()
        streams = new KafkaStreams(streamBuilder, properties)
    }

    def configureStreamBuilder() : KStreamBuilder


    override def run(): Unit = {
        streams.start()

        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
            override def run(): Unit = {
                streams.close(10, TimeUnit.SECONDS)
            }
        }))
    }
}
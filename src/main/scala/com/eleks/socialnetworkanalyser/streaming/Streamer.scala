package com.eleks.socialnetworkanalyser.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit
import com.eleks.socialnetworkanalyser.configs.StreamerConfig
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._

abstract class Streamer[TInputKey, TInputValue, TOutputKey, TOutputValue] extends Runnable {
    private var config : StreamerConfig = _
    private var streams : KafkaStreams = _

    def configure(config : StreamerConfig) {
        this.config = config

        val properties : Properties = new Properties()
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.appId)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, config.keySerde)
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, config.keySerde)
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schema)


        val streamBuilder: KStreamBuilder = new KStreamBuilder()
        val inputStream: KStream[TInputKey, TInputValue] = streamBuilder.stream(config.inputTopic)
        val outputStream: KStream[TOutputKey, TOutputValue] = transform(inputStream)
        outputStream.to(config.outputTopic)

        streams = new KafkaStreams(streamBuilder, properties)
    }

    override def run(): Unit = {
        streams.start()

        Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
            override def run(): Unit = {
                streams.close(10, TimeUnit.SECONDS)
            }
        }))
    }

    def transform(stream: KStream[TInputKey, TInputValue]) : KStream[TOutputKey, TOutputValue]
}





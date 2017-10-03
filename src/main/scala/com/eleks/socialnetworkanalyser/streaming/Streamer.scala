package com.eleks.socialnetworkanalyser.streaming

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.eleks.socialnetworkanalyser.configs.StreamerConfig
import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.eleks.socialnetworkanalyser.utilities._
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._

abstract class Streamer extends Runnable {
    private var config : StreamerConfig = _
    private var streams : KafkaStreams = _

    def configure(config : StreamerConfig) {
        this.config = config

        val properties : Properties = new Properties()
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, config.appId)
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, classOf[AvroSerde])
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, classOf[AvroSerde])

        //TODO: Put schema registryURL into config
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081")


        val streamBuilder: KStreamBuilder = new KStreamBuilder()
        val inputStream: KStream[Int, GenericRecord] = streamBuilder.stream(config.inputTopic)
        val outputStream: KStream[Int, GenericRecord] = transform(inputStream)
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

    def transform(stream: KStream[Int, GenericRecord]) : KStream[Int, GenericRecord] = {
        val inputFormatter = RecordFormat[Action]
        val outputFormatter = RecordFormat[PostStats]

        stream.map[Int, GenericRecord](new KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]]{
            override def apply(key: Int, value: GenericRecord): KeyValue[Int, GenericRecord] = {
                val action = inputFormatter.from(value)

                val newType : String = action.actionType match {
                    case "Like" => "+"
                    case "Dislike" => "-"
                    case "Repost" => "->"
                    case _ => "Unknown type"
                }


                val newAction = Action(action.actionId, action.userId, action.postId, newType);
                val outputValue = inputFormatter.to(newAction)
                new KeyValue(key, outputValue)
            }
        })
    }
}





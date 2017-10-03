package com.eleks.socialnetworkanalyser.producers

import java.util._
import com.eleks.socialnetworkanalyser.configs.ProducerConfig
import org.apache.kafka.clients.producer._

abstract class Producer extends Runnable {
    var config : ProducerConfig = _
    var producer : KafkaProducer[Any, AnyRef] = _

    def configure(config : ProducerConfig) {
        this.config = config

        val kafkaProps : Properties = new Properties()
        kafkaProps.put("bootstrap.servers", config.brokerList)
        kafkaProps.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        kafkaProps.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer")
        kafkaProps.put("schema.registry.url", config.schema)
        kafkaProps.put("producer.type", config.sync)

        producer = new KafkaProducer[Any, AnyRef](kafkaProps)
    }

    def produce(key: Any, value: AnyRef): Unit = {
        val record = new ProducerRecord[Any, AnyRef](config.topic, key, value)

        if(config.sync == "sync") {
            producer.send(record).get()
        } else {
            producer.send(record, new Callback {
                override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
                    if (exception != null) {
                        System.out.println("Error producing to topic " + metadata.topic())
                        exception.printStackTrace()
                    }
                }
            })
        }
    }

    def run()

    def close(): Unit = {
        producer.close()
    }
}

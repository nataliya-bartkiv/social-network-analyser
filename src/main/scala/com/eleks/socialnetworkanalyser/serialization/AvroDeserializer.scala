package com.eleks.socialnetworkanalyser.serialization

import java.util
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.kafka.common.serialization.Deserializer

class AvroDeserializer[T] extends Deserializer[T] {
    private val inner = new GenericAvroDeserializer()
    private var formatter : RecordFormat[T] = _

    def this(formatter : RecordFormat[T]) {
        this()
        this.formatter = formatter
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        inner.configure(configs, isKey)
    }

    override def close(): Unit = {
        inner.close()
    }

    override def deserialize(topic: String, data: Array[Byte]): T = {
        val genericRecord = inner.deserialize(topic, data)
        formatter.from(genericRecord)
    }
}
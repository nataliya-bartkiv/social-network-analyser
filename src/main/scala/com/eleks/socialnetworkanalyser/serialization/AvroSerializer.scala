package com.eleks.socialnetworkanalyser.serialization

import java.util
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import org.apache.kafka.common.serialization.Serializer

class AvroSerializer[T] extends Serializer[T] () {
    private val inner = new GenericAvroSerializer()
    private var formatter : RecordFormat[T] = _

    def this(formatter : RecordFormat[T]) {
        this()
        this.formatter = formatter
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        inner.configure(configs, isKey)
    }

    override def serialize(topic: String, data: T): Array[Byte] = {
        val genericRecord = formatter.to(data)
        inner.serialize(topic, genericRecord)
    }

    override def close(): Unit = {
        inner.close()
    }
}

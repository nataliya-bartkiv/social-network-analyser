package com.eleks.socialnetworkanalyser.serialization

import java.util
import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

class AvroSerde[T] extends Serde[T] {
        protected var formatter : RecordFormat[T] = _
        private var innerSerializer : Serializer[T] = _
        private var innerDeserializer : Deserializer[T] = _

        def this(formatter : RecordFormat[T]) {
            this()
            this.formatter = formatter

            innerSerializer = new AvroSerializer[T](formatter)
            innerDeserializer = new AvroDeserializer[T](formatter)
        }

        override def deserializer(): Deserializer[T] = innerDeserializer

        override def serializer(): Serializer[T] = innerSerializer

        override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
            val convertedConfigs : util.Map[String, String] = new util.HashMap[String, String]

            //TODO : Configure url from the outside somehow
            convertedConfigs.put("schema.registry.url", "http://localhost:8081")

            val it = configs.entrySet().iterator()
            while(it.hasNext) {
                val item = it.next().asInstanceOf[(String, String)]
                convertedConfigs.put(item._1, item._2)
            }

            serializer().configure(convertedConfigs, isKey)
            deserializer().configure(convertedConfigs, isKey)
        }

        override def close(): Unit = {
            serializer().close()
            deserializer().close()
        }
    }

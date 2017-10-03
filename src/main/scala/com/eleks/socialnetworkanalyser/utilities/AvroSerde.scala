package com.eleks.socialnetworkanalyser.utilities

import java.util
import io.confluent.kafka.serializers._
import org.apache.kafka.common.serialization._
import scala.collection.JavaConversions._

class AvroSerde extends Serde[AnyRef] {
    private var inner : Serde[AnyRef] = _

    {
        inner = Serdes.serdeFrom(new KafkaAvroSerializer(), new KafkaAvroDeserializer())
    }

    override def close(): Unit = {
        inner.serializer.close()
        inner.deserializer.close()
    }

    override def serializer: Serializer[AnyRef] = inner.serializer()

    override def deserializer: Deserializer[AnyRef] = inner.deserializer()


    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
        val convertedConfigs : util.Map[String, String] = new util.HashMap[String, String]

        //TODO : Configure url from the outside somehow
        convertedConfigs.put("schema.registry.url", "http://localhost:8081")

        for(key <- configs.keySet().iterator()) {
            val value = configs.get(key).toString
            convertedConfigs.put(key, value)
        }

        inner.serializer.configure(convertedConfigs, isKey)
        inner.deserializer.configure(convertedConfigs, isKey)
    }
}



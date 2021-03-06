package com.eleks.socialnetworkanalyser.demo

import java.util
import com.eleks.socialnetworkanalyser.entities.PostStats
import com.eleks.socialnetworkanalyser.serialization.AvroSerde
import com.sksamuel.avro4s.RecordFormat

object AvroPower {

    def main(args : Array[String]) : Unit = {
        val recordFormat = RecordFormat[PostStats]
        val serde = new AvroSerde[PostStats](recordFormat)
        serde.configure(new util.HashMap[String, String], isKey = true)
        val testPostStats = PostStats(133, 125, 16, 4)
        val serialized = serde.serializer().serialize("test-stats", testPostStats)
        println(serialized)

        val deserialized = serde.deserializer().deserialize("test-stats", serialized)
        println(deserialized)
    }

}

package com.eleks.socialnetworkanalyser.streaming

import java.lang.Long

import com.eleks.socialnetworkanalyser.entities._
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KStream, KeyValueMapper}

object UserStatsStreamer extends Streamer[Int, GenericRecord, Int, Long] {
    val inputFormatter: RecordFormat[Post] = RecordFormat[Post]
    //val outputFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    override def transform(stream: KStream[Int, GenericRecord]): KStream[Int, Long] = {
        val result = stream
                        .map[Int, GenericRecord](new KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
            override def apply(postId: Int, postRecord: GenericRecord): KeyValue[Int, GenericRecord] = {
                val userId = inputFormatter.from(postRecord).userId
                new KeyValue(userId, postRecord)
            }
        })
                .groupByKey().count().toStream()

        result.print()

        result
    }
}

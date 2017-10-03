package com.eleks.socialnetworkanalyser.streaming

import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{KStream, KeyValueMapper}

object PostStatsStreamer extends Streamer[Int, GenericRecord, Int, GenericRecord]  {
    val inputFormatter: RecordFormat[Action] = RecordFormat[Action]
    val outputFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    override def transform(stream: KStream[Int, GenericRecord]) : KStream[Int, GenericRecord] = {
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

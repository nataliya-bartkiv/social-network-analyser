package com.eleks.socialnetworkanalyser.streaming
import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.sksamuel.avro4s.RecordFormat

object PostStatsStreamer extends Streamer {
    val inputFormatter = RecordFormat[Action]
    val outputFormatter = RecordFormat[PostStats]

//    override def transform(stream: KStream[GenericRecord, GenericRecord]) : KStream[GenericRecord, GenericRecord] = {
//        import com.eleks.socialnetworkanalyser.utilities.ImplicitConversions._
//
//        stream.map[GenericRecord, GenericRecord](new KeyValueMapper[GenericRecord, GenericRecord, KeyValue[GenericRecord, GenericRecord]]{
//            override def apply(key: GenericRecord, value: GenericRecord): KeyValue[GenericRecord, GenericRecord] = {
//                val action = inputFormatter.from(value)
//
//                val newType : String = action.actionType match {
//                    case "Like" => "+"
//                    case "Dislike" => "-"
//                    case "Repost" => "->"
//                    case _ => "Unknown type"
//                }
//
//
//                val newAction = Action(action.actionId, action.userId, action.postId, newType);
//                val outputValue = inputFormatter.to(newAction)
//                 new KeyValue(key, outputValue)
//            }
//        })
//    }
}

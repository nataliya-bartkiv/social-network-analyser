package com.eleks.socialnetworkanalyser.streaming

import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

import scala.collection.mutable


object PostStatsStreamer extends Streamer[Int, GenericRecord, Int, GenericRecord]  {
    val inputFormatter: RecordFormat[Action] = RecordFormat[Action]
    val outputFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    class PostStatsInitializer extends Initializer[mutable.Map[Int, PostStats]] {
        override def apply() : mutable.Map[Int, PostStats] = {
            mutable.Map()
        }
    }

    class PostStatsAggregator extends Aggregator[Int, Action, mutable.Map[Int, PostStats]] {
        override def apply(key: Int, value: Action, aggregate: mutable.Map[Int, PostStats]): mutable.Map[Int, PostStats] = {
            var inputKey = value.postId
            var outputValue = PostStats(value.postId)

            def increment(stats : PostStats, action : Action): Unit = {
                action.actionType match {
                    case "Like" => stats.likeCount += 1
                    case "Dislike" => stats.dislikeCount += 1
                    case "Repost" => stats.repostCount += 1
                }
            }

            if(aggregate.contains(inputKey)) {
                increment(aggregate(inputKey), value)
            } else {
                val newValue = PostStats(inputKey)
                increment(newValue, value)
                aggregate(inputKey) = newValue
            }

            aggregate
        }
    }

    override def transform(stream: KStream[Int, GenericRecord]) : KStream[Int, GenericRecord] = {

        stream.map[Int, Action](new KeyValueMapper[Int, GenericRecord, KeyValue[Int, Action]] {
            override def apply(key: Int, value: GenericRecord): KeyValue[Int, Action] = {
                val action = inputFormatter.from(value)
                val postId = action.postId

                new KeyValue(postId, action)
            }
        }).groupByKey().aggregate[mutable.Map[Int, PostStats]](new PostStatsInitializer(), new PostStatsAggregator(),
            Serdes.serdeFrom[mutable.Map[Int, PostStats]](classOf[mutable.Map[Int, PostStats]]))
                .toStream()
                .map[Int, GenericRecord](new KeyValueMapper[Int, mutable.Map[Int, PostStats], KeyValue[Int, GenericRecord]] {
            override def apply(key: Int, value: mutable.Map[Int, PostStats]): KeyValue[Int, GenericRecord] = {
                val outputValue = outputFormatter.to(value(key))
                new KeyValue(key, outputValue)
            }
        })


                //.groupByKey().


        //        stream.map[Int, GenericRecord](new KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]]{
//            override def apply(key: Int, value: GenericRecord): KeyValue[Int, GenericRecord] = {
//                val action = inputFormatter.from(value)
//
//                val newType : String = action.actionType match {
//                    case "Like" => "+"
//                    case "Dislike" => "-"
//                    case "Repost" => "->"
//                    case _ => "Unknown type"
//                }
//
//                val newAction = Action(action.actionId, action.userId, action.postId, newType);
//                val outputValue = inputFormatter.to(newAction)
//                new KeyValue(key, outputValue)
//            }
//        })
    }
}

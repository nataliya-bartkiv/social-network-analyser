package com.eleks.socialnetworkanalyser.streaming

import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.eleks.socialnetworkanalyser.serialization.JSONSerde
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

import scala.collection.mutable

object PostStatsStreamer extends Streamer  {
    val inputFormatter: RecordFormat[Action] = RecordFormat[Action]
    val outputFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    class PostStatsKeyMapper extends KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
        override def apply(actionId: Int, actionRecord: GenericRecord): KeyValue[Int, GenericRecord] = {
            val postId = inputFormatter.from(actionRecord).postId
            new KeyValue(postId, actionRecord)
        }
    }

    class PostStatsInitializer extends Initializer[mutable.Map[Int, PostStats]] {
        override def apply(): mutable.Map[Int, PostStats] = {
            mutable.Map()
        }
    }

    class PostStatsAggregator extends Aggregator[Int, GenericRecord, mutable.Map[Int, PostStats]] {
        override def apply(postId: Int, actionRecord: GenericRecord, aggregate: mutable.Map[Int, PostStats]): mutable.Map[Int, PostStats] = {
            val action = inputFormatter.from(actionRecord)

            def increment(stats : PostStats, action : Action): Unit = {
                action.actionType match {
                    case "Like" => stats.likeCount += 1
                    case "Dislike" => stats.dislikeCount += 1
                    case "Repost" => stats.repostCount += 1
                }
            }

            if(aggregate.contains(postId)) {
                increment(aggregate(postId), action)
            } else {
                val stats = PostStats(postId)
                increment(stats, action)
                aggregate(postId) = stats
            }

            aggregate
        }
    }

    class PostStatsRecordMapper extends KeyValueMapper[Int, mutable.Map[Int, PostStats], KeyValue[Int, GenericRecord]] {
        override def apply(postId: Int, postStatsMap: mutable.Map[Int, PostStats]): KeyValue[Int, GenericRecord] = {
            val outputValue = outputFormatter.to(postStatsMap(postId))
            new KeyValue(postId, outputValue)
        }
    }

    def transform(stream: KStream[Int, GenericRecord]): KStream[Int, GenericRecord] = {
        stream
                .map[Int, GenericRecord](new PostStatsKeyMapper)
                .groupByKey()
                .aggregate[mutable.Map[Int, PostStats]] (
                    new PostStatsInitializer,
                    new PostStatsAggregator,
                    new JSONSerde[mutable.Map[Int, PostStats]])
                .toStream()
                .map[Int, GenericRecord](new PostStatsRecordMapper)
    }

    override def configureStreamBuilder() : KStreamBuilder = {
        val streamCount = 1
        val topics = config.inputTopics.split(";")
        if(topics.length != streamCount) {
            throw new IllegalArgumentException("There must be 1 topic to create post statistics!")
        }
        val topic = topics.head

        val streamBuilder = new KStreamBuilder()
        val stream: KStream[Int, GenericRecord] = streamBuilder.stream(topic)
        val outputStream = transform(stream)
        outputStream.to(config.outputTopic)
        streamBuilder
    }
}

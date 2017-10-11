package com.eleks.socialnetworkanalyser.streaming

import com.eleks.socialnetworkanalyser.entities.{Action, PostStats}
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

object PostStatsStreamer extends Streamer  {
    val actionFormatter: RecordFormat[Action] = RecordFormat[Action]
    val statsFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    class PostStatsMapper extends KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
        override def apply(actionId: Int, actionRecord: GenericRecord) : KeyValue[Int, GenericRecord] = {
            val action = actionFormatter.from(actionRecord)
            val stats = PostStats(action.postId)

            action.actionType match {
                case "Like" => stats.likeCount += 1
                case "Dislike" => stats.dislikeCount += 1
                case "Repost" => stats.repostCount += 1
            }

            val statsRecord = statsFormatter.to(stats)
            new KeyValue(action.postId, statsRecord)
        }
    }

    class PostStatsReducer extends Reducer[GenericRecord] {
        override def apply(value1: GenericRecord, value2: GenericRecord) : GenericRecord = {
            val one = statsFormatter.from(value1)
            val two = statsFormatter.from(value2)
            val result = one + two

            statsFormatter.to(result)
        }
    }


    def transform(stream: KStream[Int, GenericRecord]): KStream[Int, GenericRecord] = {
        val mapped: KStream[Int, GenericRecord] = stream
                .map[Int, GenericRecord](new PostStatsMapper)
                .through("post-stats-mapped")
        mapped
                .groupByKey()
                .reduce(new PostStatsReducer , "post-stats-reducer")
                .toStream()
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

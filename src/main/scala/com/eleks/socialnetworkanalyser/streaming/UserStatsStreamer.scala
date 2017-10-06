package com.eleks.socialnetworkanalyser.streaming

import java.lang.Long
import java.util.concurrent.TimeUnit

import com.eleks.socialnetworkanalyser.entities._
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

object UserStatsStreamer extends Streamer[Int, GenericRecord, Int, Long] {
    val inputFormatter: RecordFormat[Post] = RecordFormat[Post]
    val outputFormatter: RecordFormat[PostStats] = RecordFormat[PostStats]

    override def transform(stream: KStream[Int, GenericRecord]): KStream[Int, Long] = {
        stream
        .map[Int, GenericRecord](new KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
            override def apply(postId: Int, postRecord: GenericRecord): KeyValue[Int, GenericRecord] = {
                val userId = inputFormatter.from(postRecord).userId
                new KeyValue(userId, postRecord)
            }
        })
        .groupByKey()
        .count()
        .toStream()
    }

    /**
      * Joins two streams: users and posts
      * users are needed to get names and surnames
      * posts are transformed with transform(...) to get posts count per each user
      * The order of topics in config IS IMPORTANT!
      * @return
      */
    override def configureStreamBuilder(): KStreamBuilder = {
        //Some hardcoded configs
        val streamCount = 2;

        val topics = config.inputTopics.split(";")
        if(topics.length != 2) {
            throw new IllegalArgumentException("There must be 2 topics to create user statistics!")
        }

        val streamBuilder : KStreamBuilder = new KStreamBuilder()
        val userStream : KStream[Int, GenericRecord] = streamBuilder.stream(topics(0)) //Getting stream for the first topic - must be users topic
        val postStream : KStream[Int, GenericRecord] = streamBuilder.stream(topics(1)) //Getting stream for the  second topic - must be posts topic
        val statsStream = transform(postStream)

        val result = userStream.join[Long, GenericRecord](
            statsStream,
            new ValueJoiner[GenericRecord, Long, GenericRecord] {
                override def apply(value1: GenericRecord, value2: Long): GenericRecord = {
                    val user = RecordFormat[User].from(value1)

                    val userStats = UserStats(user.name, user.surname, value2.toInt)
                    RecordFormat[UserStats].to(userStats)
                }
            },
            JoinWindows.of(TimeUnit.SECONDS.toMillis(5))
        )

        result.to(config.outputTopic)
        streamBuilder
    }
}

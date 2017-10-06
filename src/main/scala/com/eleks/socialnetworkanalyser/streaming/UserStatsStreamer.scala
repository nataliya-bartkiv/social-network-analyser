package com.eleks.socialnetworkanalyser.streaming

import java.lang.Long
import java.util.concurrent.TimeUnit

import com.eleks.socialnetworkanalyser.entities._
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._

object UserStatsStreamer extends Streamer[Int, GenericRecord, Int, GenericRecord] {
    val postFormatter: RecordFormat[Post] = RecordFormat[Post]
    val userFormatter: RecordFormat[User] = RecordFormat[User]
    val statsFormatter: RecordFormat[UserStats] = RecordFormat[UserStats]

    class PostKeyMapper extends KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
        override def apply(postId: Int, postRecord: GenericRecord): KeyValue[Int, GenericRecord] = {
            val user = postFormatter.from(postRecord)
            val userId = user.userId
            new KeyValue(userId, postRecord)
        }
    }

    class UserStatsValueJoiner extends ValueJoiner[GenericRecord, Long, GenericRecord] {
        override def apply(userRecord: GenericRecord, postCount: Long): GenericRecord = {
            val user = userFormatter.from(userRecord)

            val userStats = UserStats(user.name, user.surname, postCount)
            statsFormatter.to(userStats)
        }
    }

    override def transform(streams: List[KStream[Int, GenericRecord]]): KStream[Int, GenericRecord] = {
        val streamCount = 2

        if(streams.length != streamCount) {
            throw new IllegalArgumentException("There must be 2 topics to create user statistics!")
        }

        val postStream = streams.head
        val userStream = streams(1)

        val statsStream = postStream
            .map[Int, GenericRecord](new PostKeyMapper)
            .groupByKey()
            .count()
            .toStream()

        val result = userStream.join[Long, GenericRecord](
            statsStream,
            new UserStatsValueJoiner,
            JoinWindows.of(TimeUnit.SECONDS.toMillis(5))
        )

        result
    }
}

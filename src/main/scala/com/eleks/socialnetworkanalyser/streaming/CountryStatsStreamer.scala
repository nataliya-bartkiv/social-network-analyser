package com.eleks.socialnetworkanalyser.streaming

import java.util.concurrent.TimeUnit

import com.eleks.socialnetworkanalyser.entities._
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream._
import java.lang.Long

object CountryStatsStreamer extends Streamer {
    val userFormatter : RecordFormat[User] = RecordFormat[User]
    val postFormatter : RecordFormat[Post] = RecordFormat[Post]
    val statsFormatter : RecordFormat[CountryStats] = RecordFormat[CountryStats]

    class PostKeyMapper extends KeyValueMapper[Int, GenericRecord, KeyValue[Int, GenericRecord]] {
        override def apply(postId: Int, postRecord: GenericRecord): KeyValue[Int, GenericRecord] = {
            val user = postFormatter.from(postRecord)
            val userId = user.userId
            new KeyValue(userId, postRecord)
        }
    }

    class UserPostValueJoiner extends ValueJoiner[GenericRecord, GenericRecord, String] {
        override def apply(userRecord: GenericRecord, postRecord: GenericRecord): String = {
            val user = userFormatter.from(userRecord)
            user.country
        }
    }

    class CountryKeyMapper extends KeyValueMapper[Int, String, KeyValue[String, String]] {
        override def apply(userId: Int, country: String) : KeyValue[String, String] = {
            new KeyValue(country, country)
        }
    }

    class CountryStatsKeyValueMapper extends KeyValueMapper[String, Long, KeyValue[String, GenericRecord]] {
        override def apply(country: String, postCount: Long): KeyValue[String, GenericRecord] = {
            val countryStats = CountryStats(country, postCount)
            val statsRecord = statsFormatter.to(countryStats)
            new KeyValue(country, statsRecord)
        }
    }

    override def configureStreamBuilder() : KStreamBuilder = {
        val streamBuilder = new KStreamBuilder()

        val streamCount = 2
        val topics = config.inputTopics.split(";")
        if(topics.length != streamCount) {
            throw new IllegalArgumentException("There must be 2 topics to create user statistics!")
        }

        val userTable: KStream[Int, GenericRecord] = streamBuilder.stream(topics.head)
        val postStream: KStream[Int, GenericRecord] = streamBuilder.stream(topics(1))
        val outputStream = transform(userTable, postStream)
        outputStream.to(config.outputTopic)

        streamBuilder
    }

    def transform(userStream: KStream[Int, GenericRecord], postStream: KStream[Int, GenericRecord]): KStream[String, GenericRecord] = {
        val modifiedPostStream: KStream[Int, GenericRecord] = postStream.map[Int, GenericRecord] (new PostKeyMapper)


        //TODO : Somehow get ALL users, not only once produced earlier
        userStream.join[GenericRecord, String] (
            modifiedPostStream,
            new UserPostValueJoiner,
            JoinWindows.of(TimeUnit.HOURS.toMillis(1))
        )
            .map[String, String](new CountryKeyMapper)
            .groupByKey()
            .count()
            .toStream()
            .map[String, GenericRecord](new CountryStatsKeyValueMapper)
    }
}

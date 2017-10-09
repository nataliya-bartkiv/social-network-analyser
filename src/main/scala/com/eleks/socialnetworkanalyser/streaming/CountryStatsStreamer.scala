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

    class PostKeyMapper extends KeyValueMapper[Int, GenericRecord, Int] {
        override def apply(key: Int, value: GenericRecord) : Int = {
            val post = postFormatter.from(value)
            post.userId
        }
    }

    class PostUserValueJoiner extends ValueJoiner[GenericRecord, GenericRecord, String] {
        override def apply(post: GenericRecord, userRecord: GenericRecord): String = {
            val user = userFormatter.from(userRecord)
            user.country
        }
    }

    class CountryKeyMapper extends KeyValueMapper[Int, String, String] {
        override def apply(key: Int, value: String) : String = {
            value
        }
    }

    class CountryStatsKeyValueMapper extends KeyValueMapper[Windowed[String], Long, KeyValue[String, GenericRecord]] {
        override def apply(windowedCountry: Windowed[String], postCount: Long): KeyValue[String, GenericRecord] = {
            val country = windowedCountry.key()
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

        val userTable: KTable[Int, GenericRecord] = streamBuilder.table(topics.head)
        val postStream: KStream[Int, GenericRecord] = streamBuilder.stream(topics(1))
        val outputStream = transform(userTable, postStream)
        outputStream.to(config.outputTopic)

        streamBuilder
    }

    def transform(userTable: KTable[Int, GenericRecord], postStream: KStream[Int, GenericRecord]): KStream[String, GenericRecord] = {
        postStream
                .selectKey[Int](new PostKeyMapper)
                .join[GenericRecord, String] (
                    userTable,
                    new PostUserValueJoiner
                )
                .selectKey[String](new CountryKeyMapper)
                .groupByKey()
                .count(TimeWindows.of(TimeUnit.HOURS.toMillis(1)))
                .toStream()
                .map[String, GenericRecord](new CountryStatsKeyValueMapper)
    }
}

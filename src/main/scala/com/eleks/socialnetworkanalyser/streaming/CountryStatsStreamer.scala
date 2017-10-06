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

    override def configureStreamBuilder() : KStreamBuilder = {
        val streamBuilder = new KStreamBuilder()

        val streamCount = 2
        val topics = config.inputTopics.split(";")
        if(topics.length != streamCount) {
            throw new IllegalArgumentException("There must be 2 topics to create user statistics!")
        }

        val userStream: KStream[Int, GenericRecord] = streamBuilder.stream(topics.head)
        val postStream: KStream[Int, GenericRecord] = streamBuilder.stream(topics(1))
        val outputStream = transform(userStream, postStream)
        outputStream.to(config.outputTopic)

        streamBuilder
    }

    def transform(userStream: KStream[Int, GenericRecord], postStream: KStream[Int, GenericRecord]): KStream[String, GenericRecord] = {
        val modifiedPostStream = postStream.map[Int, GenericRecord] (new PostKeyMapper)

        userStream.join[GenericRecord, String] (
            modifiedPostStream,
            new ValueJoiner[GenericRecord, GenericRecord, String] {
                override def apply(value1: GenericRecord, value2: GenericRecord): String = {
                    val user = userFormatter.from(value1)
                    user.country
                }
            },
            JoinWindows.of(TimeUnit.HOURS.toMillis(1))
        ).map[String, String](new KeyValueMapper[Int, String, KeyValue[String, String]] {
            override def apply(key: Int, value: String) : KeyValue[String, String] = {
                new KeyValue(value, value)
            }
        }).groupByKey().count().toStream()
        .map[String, GenericRecord](new KeyValueMapper[String, Long, KeyValue[String, GenericRecord]] {
            override def apply(key: String, value: Long) : KeyValue[String, GenericRecord] = {
                val countryStats = CountryStats(key, value)
                val statsRecord = statsFormatter.to(countryStats)
                new KeyValue(key, statsRecord)
            }
        })
    }
}

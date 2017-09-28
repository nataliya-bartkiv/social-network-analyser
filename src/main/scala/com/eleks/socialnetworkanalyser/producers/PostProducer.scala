package com.eleks.socialnetworkanalyser.producers

import com.eleks.socialnetworkanalyser.entities.Post
import com.eleks.socialnetworkanalyser.generators.PostGenerator
import com.sksamuel.avro4s.RecordFormat

import scala.util.Random

object PostProducer extends Producer {
    val formatter: RecordFormat[Post] = RecordFormat[Post]

    override def run(): Unit = {
        try {
            while (!Thread.currentThread.isInterrupted) {
                val instantPostsCount = 3 + Random.nextInt(3); //From 3 to 5 posts per second
                for (_ <- 0 until instantPostsCount) {
                    val post = PostGenerator.next()
                    val formattedPost = formatter.to(post)
                    produce(post.postId, formattedPost)
                }
                Thread.sleep(1000)
            }

        } catch {
            case ex: Throwable =>
                this.close()
                println(ex.getMessage)
        }
    }
}

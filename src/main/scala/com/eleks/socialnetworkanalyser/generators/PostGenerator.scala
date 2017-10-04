package com.eleks.socialnetworkanalyser.generators

import java.util.concurrent.atomic.AtomicInteger
import com.eleks.socialnetworkanalyser.entities.Post
import scala.io.Source
import scala.util.Random

object PostGenerator extends Generator[Post] {
    val usersPath = "data/users.txt"
    val contentPath = "data/content.txt"

    var currentId : AtomicInteger = new AtomicInteger()
    var usersCount : Int = Source.fromFile(usersPath).getLines().length
    var contentCount : Int = Source.fromFile(contentPath).getLines().length

    override def next() : Post = {
        val userId = Random.nextInt(usersCount) + 1
        currentId.incrementAndGet()

        val contentNumber = Random.nextInt(contentCount)
        val lines = Source.fromFile(contentPath).getLines()
        for(_ <- 0 until contentNumber) {
            lines.next()
        }
        val content = lines.next()

        Post(currentId.get(), userId, content)
    }
}

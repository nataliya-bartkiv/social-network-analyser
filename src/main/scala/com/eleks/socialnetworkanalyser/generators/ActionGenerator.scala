package com.eleks.socialnetworkanalyser.generators

import com.eleks.socialnetworkanalyser.entities.{Action, ActionType}

import scala.io.Source
import scala.util.Random

object ActionGenerator extends Generator[Action] {
    val usersPath = "data/users.txt"

    var currentId : Int = 0
    var usersCount : Int = Source.fromFile(usersPath).getLines().length
    var postsCount : Int = 0

    override def next() : Action = {
        currentId += 1

        val userId: Int = Random.nextInt(usersCount) + 1

        val postsCount = PostGenerator.currentId.get()
        val postId = {
            if (postsCount > 0)
                Random.nextInt(postsCount)
            else
                postsCount
        }

        val actionIndex = Random.nextInt(ActionType.types.length)
        val action = ActionType.types(actionIndex)

        Action(currentId, postId, userId, action)
    }
}

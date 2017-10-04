package com.eleks.socialnetworkanalyser.producers

import com.eleks.socialnetworkanalyser.entities.User
import com.sksamuel.avro4s.RecordFormat
import scala.io.Source

object UserProducer extends Producer {

    val sourceFilepath = "data/users.txt"
    val formatter: RecordFormat[User] = RecordFormat[User]

    override def run(): Unit = {
        try {
            Source.fromFile(sourceFilepath).getLines().foreach(line => {
                val values: Array[String] = line.split("\t")

                //Some more hardcode
                if (values.length != 5) {
                    throw new IllegalArgumentException("Incorrect file content!")
                }

                val id = values(0).toInt
                val name = values(1)
                val surname = values(2)
                val city = values(3)
                val country = values(4)

                val user = User(id, name, surname, city, country)
                val formattedUser = formatter.to(user)
                produce(id, formattedUser)
            })

        } catch {
            case ex: Throwable =>
                this.close()
                println(ex.getMessage)

        }
    }
}

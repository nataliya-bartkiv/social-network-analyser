package com.eleks.socialnetworkanalyser.producers

import com.eleks.socialnetworkanalyser.entities.Action
import com.eleks.socialnetworkanalyser.generators.ActionGenerator
import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.generic.GenericRecord

import scala.util.Random

object ActionProducer extends Producer {
    val formatter: RecordFormat[Action] = RecordFormat[Action]

    override def run(): Unit = {
        try {
            while (!Thread.currentThread.isInterrupted) {
                val instantActionsCount = 3 + Random.nextInt(3); //From 3 to 5 actions per second
                for (_ <- 0 until instantActionsCount) {
                    val action = ActionGenerator.next()
                    val formattedAction: GenericRecord = formatter.to(action)
                    produce(action.actionId, formattedAction)
                }

                Thread.sleep(1000)
            }
        } catch {
            case ex: Throwable =>
                this.close()
                println(ex.getMessage)
        } finally {

        }
    }
}

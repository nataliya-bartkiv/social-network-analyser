package com.eleks.socialnetworkanalyser

import com.eleks.socialnetworkanalyser.producers._
import com.eleks.socialnetworkanalyser.utilities._

object Main {
    def main(args : Array[String]) : Unit = {

        val config = ConfigParser.parse(args)

        if(config == null) {
            return
        }

        UserProducer.configure(ProducerConfigManager.getUserConfig(config))
        val userProducer = new Thread(UserProducer)

        PostProducer.configure(ProducerConfigManager.getPostConfig(config))
        val postProducer = new Thread(PostProducer)

        ActionProducer.configure(ProducerConfigManager.getActionConfig(config))
        val actionProducer = new Thread(ActionProducer)

        userProducer.start()
        postProducer.start()
        actionProducer.start()
    }
}

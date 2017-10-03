package com.eleks.socialnetworkanalyser

import com.eleks.socialnetworkanalyser.configs._
import com.eleks.socialnetworkanalyser.producers._
import com.eleks.socialnetworkanalyser.streaming.PostStatsStreamer

object Main {
    def main(args : Array[String]) : Unit = {

        val config = ConfigParser.parse(args)

        if(config == null) {
            return
        }

        PostStatsStreamer.configure(StreamingConfigManager.getPostStatsConfig(config))
        val postStatsStreamer = new Thread(PostStatsStreamer)

        UserProducer.configure(ProducerConfigManager.getUserConfig(config))
        val userProducer = new Thread(UserProducer)

        PostProducer.configure(ProducerConfigManager.getPostConfig(config))
        val postProducer = new Thread(PostProducer)

        ActionProducer.configure(ProducerConfigManager.getActionConfig(config))
        val actionProducer = new Thread(ActionProducer)

        postStatsStreamer.start()
        //userProducer.start()
        //postProducer.start()
        actionProducer.start()
    }
}

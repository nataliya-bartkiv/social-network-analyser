package com.eleks.socialnetworkanalyser.configs

import scopt._

object ConfigParser {
    var coreObj : OptionParser[Config] = _

    private def createCore() : OptionParser[Config] = {
        new scopt.OptionParser[Config]("SocialNetworkProducers") {
            override def renderingMode: RenderingMode = scopt.RenderingMode.OneColumn

            head("Social Network Producers", "0.x")
            note("allows to produce random users, posts and post actions to three different topic")

            opt[String]('b', "brokerList")
                    .action((x, c) => c.copy(brokerList = x))
                    .text("list of brokers - network nodes")

            opt[String]('s', "sync")
                    .valueName("<sync type (sync/async)>")
                    .action((x, c) => c.copy(sync = x))
                    .validate(x =>
                        if (x == "sync" || x == "async") success
                        else failure("Wrong sync type!")
                    )
                    .text("synchronization type")

            opt[String]("schema-registry-url")
                    .action((x, c) => c.copy(schema = x))
                    .text("schema registry url")

            opt[String]("key-serializer")
                    .action((x, c) => c.copy(keySerializer = x))
                    .text("class which will be used for key serialization")

            opt[String]("value-serializer")
                    .action((x, c) => c.copy(valueSerializer = x))
                    .text("class which will be used for value serialization")

//            cmd("help")
//                    .action((_, config) => {
//                       this.notifyAll();
//                        config
//                    })
//                    .text("general info about params")
        }
    }

    def core() : OptionParser[Config] = {
        if(coreObj == null) coreObj = createCore()
        coreObj
    }

    def parse(args : Array[String]) : Config = {
        core().parse(args, Config()) match {
            case Some(config) =>
                config
            case None =>
                null
            //throw new IllegalArgumentException("Cannot parse the arguments!");
        }
    }
}

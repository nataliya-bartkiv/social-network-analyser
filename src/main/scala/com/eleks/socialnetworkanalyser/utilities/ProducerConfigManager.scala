package com.eleks.socialnetworkanalyser.utilities

object ProducerConfigManager {
    def getUserConfig(config : Config) : ProducerConfig = {
        ProducerConfig(
            config.brokerList, config.sync, config.schema,
            config.keySerializer, config.valueSerializer,
            "users"
        )
    }

    def getPostConfig(config : Config) : ProducerConfig = {
        ProducerConfig(
            config.brokerList, config.sync, config.schema,
            config.keySerializer, config.valueSerializer,
            "posts"
        )
    }

    def getActionConfig(config : Config) : ProducerConfig = {
        ProducerConfig(
            config.brokerList, config.sync, config.schema,
            config.keySerializer, config.valueSerializer,
            "actions"
        )
    }
}

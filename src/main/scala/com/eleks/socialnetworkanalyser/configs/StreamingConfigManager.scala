package com.eleks.socialnetworkanalyser.configs

import com.eleks.socialnetworkanalyser.utilities.AvroSerde

object StreamingConfigManager {
    def getPostStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "post-statistics", config.brokerList,
            classOf[AvroSerde].getName, classOf[AvroSerde].getName,
            "actions", "post-stats",
            config.schema
        )
    }

    def getUserStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "user-statistics", config.brokerList,
            classOf[AvroSerde].getName, classOf[AvroSerde].getName,
            "users;posts", "user-stats",
            config.schema
        )
    }

    def getCountryStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "country-statistics", config.brokerList,
            classOf[AvroSerde].getName, classOf[AvroSerde].getName,
            "posts", "country-stats",
            config.schema
        )
    }
}

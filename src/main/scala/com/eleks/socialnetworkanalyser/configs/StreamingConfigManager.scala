package com.eleks.socialnetworkanalyser.configs

import com.eleks.socialnetworkanalyser.utilities.AvroSerde

object StreamingConfigManager {
    def getPostStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "post-statistics", config.brokerList,
            classOf[AvroSerde].toString, classOf[AvroSerde].toString,
            "actions", "post-stats"
        )
    }

    def getUserStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "user-statistics", config.brokerList,
            classOf[AvroSerde].toString, classOf[AvroSerde].toString,
            "posts", "user-stats"
        )
    }

    def getCountryStatsConfig(config: Config): StreamerConfig = {
        StreamerConfig(
            "country-statistics", config.brokerList,
            classOf[AvroSerde].toString, classOf[AvroSerde].toString,
            "posts", "country-stats"
        )
    }

}

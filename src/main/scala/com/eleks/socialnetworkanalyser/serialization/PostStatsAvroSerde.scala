package com.eleks.socialnetworkanalyser.serialization

import com.eleks.socialnetworkanalyser.entities.PostStats
import com.sksamuel.avro4s._

class PostStatsAvroSerde extends AvroSerde[PostStats] {
    formatter = RecordFormat[PostStats]
}

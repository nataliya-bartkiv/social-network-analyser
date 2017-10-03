package com.eleks.socialnetworkanalyser.utilities

import org.apache.kafka.streams.KeyValue
import scala.language.implicitConversions


object ImplicitConversions {
    implicit def Tuple2ToKeyValue[TKey, TValue](tuple : (TKey, TValue)) : KeyValue[TKey, TValue] = {
        new KeyValue(tuple._1, tuple._2)
    }


}

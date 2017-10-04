package com.eleks.socialnetworkanalyser.generators

abstract class Generator[Type] {
    def next() : Type
}

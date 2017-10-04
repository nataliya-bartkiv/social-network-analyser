package com.eleks.socialnetworkanalyser.entities

case class PostStats(var postId : Int = 0,
                     var likeCount: Int = 0,
                     var dislikeCount : Int = 0,
                     var repostCount : Int = 0
                    )

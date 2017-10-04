package com.eleks.socialnetworkanalyser.entities

case class PostStats(var postId : Int = 0,
                     var likeCount: Int = 0,
                     var dislikeCount : Int = 0,
                     var repostCount : Int = 0
                    ) {
    def + (post : PostStats) : PostStats = {
        PostStats(this.postId,
            this.likeCount + post.likeCount,
            this.dislikeCount + post.dislikeCount,
            this.repostCount + post.repostCount)
    }

}

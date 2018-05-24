package demy.mllib.text

import demy.mllib.linalg.SemanticVector

case class SemanticPhrase(docId:Int, phraseId:Int, words:Seq[Word], phraseSemantic:Option[SemanticVector]=None, clusterId:Option[Int]=None, centerDistance:Option[Double]=None, hierarchy:Seq[Int]=Seq[Int]()) {
    def mergeWith(that:SemanticPhrase) = SemanticPhrase(this.docId, this.phraseId, (this.words ++ that.words).sortWith(_.index<_.index)
                                        , (this.phraseSemantic, that.phraseSemantic) match {
                                            case (Some(a),Some(b)) => Some(a.sum(b))
                                            case (Some(a),None) => Some(a)
                                            case (None, Some(b)) => Some(b)
                                            case _ => None
                                        } )
    def deleteWordSemantic = SemanticPhrase(this.docId, this.phraseId, this.words.map(w => w.setSemantic(null)), this.phraseSemantic, this.clusterId, this.centerDistance, this.hierarchy)
    def setCenter(clusterId:Int, center:Cluster) = {
        val distance = this.phraseSemantic match { case Some(v) => Some(1-center.center.cosineSimilarity(v)) case _=> None}
        SemanticPhrase(this.docId, this.phraseId, this.words, this.phraseSemantic, Some(clusterId), distance, this.hierarchy)
    }
    def setHierarchy(hierarchy:Seq[Int]) =  SemanticPhrase(this.docId, this.phraseId, this.words, this.phraseSemantic, this.clusterId, this.centerDistance, hierarchy)
}
trait PhraseRef {
    val docId:Int
    val phraseId:Int
}
trait WithSemantic  {
    val semantic:SemanticVector
//    def setSemantic(semantic:SemanticVector) : WithSemantic
}
trait WithTagged  {
    val isTag:Seq[String]
    val isNotTag:Seq[String]
    def setTagged(isTag:Seq[String], isNotTag:Seq[String]):WithTagged
    def addTagsWith(that:WithTagged) = setTagged(isTag = (this.isTag ++ that.isTag).distinct, isNotTag = (this.isNotTag ++ that.isNotTag).distinct)
}
trait WithTaggedPhrase extends PhraseRef with WithTagged {
    def setTaggedPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String]):WithTaggedPhrase 
    def setTagged(isTag:Seq[String], isNotTag:Seq[String]) = setTaggedPhrase(isTag = isTag, isNotTag = isNotTag, docId = this.docId, phraseId = phraseId)
    def toTaggedSemantic(semantic:SemanticVector) = TaggedSemantic(isTag = this.isTag, isNotTag = this.isNotTag, semantic = semantic)
}
trait WithSemanticTagPhrase extends WithTaggedPhrase with WithSemantic {
    def setSemanticTagPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String], semantic:SemanticVector):WithSemanticTagPhrase 
    def setTaggedPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String])= setSemanticTagPhrase(docId = docId, phraseId= phraseId, isTag = isTag, isNotTag = isNotTag, semantic = this.semantic)
}
case class SemanticTagPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String], semantic:SemanticVector) extends WithSemanticTagPhrase {
    def setSemanticTagPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String], semantic:SemanticVector)
            = SemanticTagPhrase(docId = docId, phraseId= phraseId, isTag = isTag, isNotTag = isNotTag, semantic = semantic)
}
trait WithTags {
    val tags:Seq[String]
    def setTags(tags:Seq[String]):WithTags
}
;trait WithTagsSemantic extends WithTags with WithSemantic {
    def setTagsSemantic(semantic:SemanticVector, tags:Seq[String]):WithTagsSemantic
    def setTags(tags: Seq[String]) =  setTagsSemantic(semantic= this.semantic, tags = tags) 

};case class TagsSemantic(semantic:SemanticVector, tags:Seq[String]) extends WithTagsSemantic {
    def setTagsSemantic(semantic:SemanticVector, tags:Seq[String]) = TagsSemantic(semantic = semantic, tags = tags)
};case class TaggedPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String]) extends WithTaggedPhrase {
    def setTaggedPhrase(docId:Int, phraseId:Int, isTag:Seq[String], isNotTag:Seq[String]) = TaggedPhrase(docId = docId, phraseId = phraseId, isTag = isTag, isNotTag = isNotTag)
};object TaggedPhrase {
    def userContext2Dataset(userContextPath:Seq[String], spark:org.apache.spark.sql.SparkSession) = {
        import spark.implicits._
        val phrases = userContextPath.zipWithIndex.map(p => p match { case (path, index) => {
            val contextSTR = spark.sparkContext.textFile(path).collect.mkString("\n")
            val parsed = scala.util.parsing.json.JSON.parseFull(contextSTR)
            val nomatch = Seq[TaggedPhrase]()
            val taggedPhrases = parsed match {
                case Some(map:Map[_,_]) => map.asInstanceOf[Map[String, Any]]("taggedPhrases") match {
                    case tn:Map[_,_] => {
                        val taggedPhrases = tn.asInstanceOf[Map[String, Boolean]]
                        taggedPhrases.toSeq.map(p => p match { case (phraseKey, tagged) => {
                                                                        var lim1 = phraseKey.indexOf("@").toInt 
                                                                        var lim2 = phraseKey.indexOf("@", lim1+1).toInt 
                                                                        TaggedPhrase(docId = phraseKey.substring(0, lim1).toInt
                                                                                    , phraseId = phraseKey.substring(lim1+1, lim2).toInt
                                                                                    , isTag = if(tagged) Seq(phraseKey.substring(lim2+1))
                                                                                       else Seq[String]()
                                                                                    , isNotTag = if(!tagged) Seq(phraseKey.substring(lim2+1))
                                                                                       else Seq[String]()
                                            )}})
                    }
                    case _ => nomatch
                }
                case _ => nomatch
            }
            spark.sparkContext.parallelize(taggedPhrases).toDS
                .groupByKey(p => (p.docId, p.phraseId))
                .reduceGroups((p1, p2) => p1.addTagsWith(p2).asInstanceOf[TaggedPhrase])
                .map(p => (p._2, index))
        }})
        
        val phrasesAllContext = phrases.reduce((ds1, ds2) => ds1.union(ds2))
        phrasesAllContext
           .groupByKey(p => p match{case (phrase, index) => (phrase.phraseId, phrase.docId)})
           .reduceGroups((p1, p2) => (p1, p2) match {case ((phrase1, index1), (phrase2, index2)) => (TaggedPhrase(docId = phrase1.docId, phraseId = phrase1.phraseId
                                                                                                            , isTag = phrase1.isTag.toSet.union(phrase2.isTag.toSet).toSeq
                                                                                                            , isNotTag = phrase1.isNotTag.toSet.union(phrase2.isNotTag.toSet)
                                                                                                                            .diff(phrase1.isTag.toSet.union(phrase2.isTag.toSet))
                                                                                                                            .toSeq
                                                                                                        ), if(index1 > index2) index1 else index2)})
            .map(p => p._2._1)
        
    }
};trait WithTaggedSemantic extends WithTagged with WithSemantic {
    def setTaggedSemantic(semantic:SemanticVector, isTag:Seq[String], isNotTag:Seq[String]):WithTaggedSemantic
    def setTagged(isTag:Seq[String], isNotTag:Seq[String]) = setTaggedSemantic(semantic = this.semantic, isTag = isTag, isNotTag = isNotTag)
};case class TaggedSemantic(semantic:SemanticVector, isTag:Seq[String], isNotTag:Seq[String]) extends WithTaggedSemantic {
    def setTaggedSemantic(semantic:SemanticVector, isTag:Seq[String], isNotTag:Seq[String]) = TaggedSemantic(semantic = semantic, isTag = isTag, isNotTag = isNotTag)
    
};trait WithPhraseSemantic extends PhraseRef with WithSemantic {
    def setCenter(center:WithCenterSemantic, tags:Seq[String]) = PhraseOnCluster(docId = this.docId, phraseId = this.phraseId, semantic = this.semantic, clusterId = center.centerId, distance = 1 - center.center.cosineSimilarity(this.semantic), tags = tags)
};case class PhraseSemantic(docId:Int, phraseId:Int, semantic:SemanticVector) extends WithPhraseSemantic {
};trait WithPhraseOnCluster extends WithPhraseSemantic {
    val clusterId:Int
    val distance:Double
    val tags:Seq[String]
    def toCenterStatsByDoc() = CenterStatsByDoc(centerId = this.clusterId, docId = this.docId, docCount = 1, phraseCount = 1, totalDistance = this.distance)
    def toCenterSemanticStats() = CenterSemanticStats(centerId = this.clusterId, phraseCount = 1, center = this.semantic)
    def toCenterTagged(keepId:Boolean = false) = CenterTagged(centerId = if(keepId) this.clusterId else -1, center = this.semantic, tags= this.tags)    
    def toPhraseSemantic(hierarchy:Seq[Int]) = SemanticPhrase(docId=this.docId, phraseId=this.phraseId, words = Seq[Word](), phraseSemantic = Some(this.semantic)
                                , clusterId = Some(this.clusterId), centerDistance = Some(this.distance), hierarchy=hierarchy)
};case class PhraseOnCluster(docId:Int, phraseId:Int, semantic:SemanticVector, clusterId:Int, distance:Double, tags:Seq[String]) extends WithPhraseOnCluster {
//    def setPhraseOnCluster(docId:Int, phraseId:Int, semantic:SemanticVector, clusterId:Int, distance:Double) = PhraseOnCluster(docId = docId, phraseId = phraseId, semantic = semantic, clusterId = clusterId, distance = distance)
    def setCenter(center:WithCenterSemantic) = PhraseOnCluster(docId = this.docId, phraseId = this.phraseId, semantic = this.semantic, clusterId = center.centerId, distance = 1 - center.center.cosineSimilarity(this.semantic), tags = this.tags)
};case class ClusteringTagQuality(previousClusterTags:Int=0, maintainedClusterTags:Int=0, previousPhrasesInTags:Int=0, maintainedPhraseInTags:Int=0, previousPhrasesOutTags:Int=0, maintainedPhraseOutTags:Int=0) {
    def reduceWith(that:ClusteringTagQuality) = ClusteringTagQuality(previousClusterTags = this.previousClusterTags + that.previousClusterTags, maintainedClusterTags = this.maintainedClusterTags + that.maintainedClusterTags
                                                        , previousPhrasesInTags = this.previousPhrasesInTags + that.previousPhrasesInTags, maintainedPhraseInTags = this.maintainedPhraseInTags + that.maintainedPhraseInTags
                                                        , previousPhrasesOutTags = this.previousPhrasesOutTags + that.previousPhrasesOutTags, maintainedPhraseOutTags = this.maintainedPhraseOutTags + that.maintainedPhraseOutTags)
    def toMessage = s"ClusteringTagQuality(previousClusterTags ${this.previousClusterTags}, maintainedClusterTags = ${this.maintainedClusterTags}, previousPhrasesInTags = ${this.previousPhrasesInTags}, maintainedPhraseInTags = ${this.maintainedPhraseInTags}, previousPhrasesOutTags = ${this.previousPhrasesOutTags}, maintainedPhraseOutTags = ${this.maintainedPhraseOutTags})"
}

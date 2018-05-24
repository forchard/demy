package demy.mllib.text

import demy.mllib.linalg.SemanticVector
import demy.mllib.linalg.Coordinate

case class Cluster(id:Int, center:SemanticVector, count:Int, clusterCost:Double, parents:scala.collection.mutable.ArrayBuffer[Int]=scala.collection.mutable.ArrayBuffer[Int]()
        , var parentCenter:SemanticVector=null, var parentDiff:SemanticVector=null, hierarchy:Seq[Int]=null, topWord:String=null, topWords:Seq[Word]=null, tags:Seq[String]=Seq[String]()) extends WithHierarchy {
    def averageWith(that:Cluster, newId:Int = -1) = Cluster(if(newId<0)this.id else newId, this.center.scale(1.0*this.count/(this.count+that.count)).sum(that.center.scale(1.0*that.count/(this.count+that.count))), this.count+that.count, (this.clusterCost+that.clusterCost), scala.collection.mutable.ArrayBuffer[Int](), this.parentCenter, this.parentDiff, this.hierarchy, this.topWord, this.topWords, this.tags.filter(t => that.tags.contains(t)))
    def averageCenter(that:Cluster, newId:Int = -1) = Cluster(if(newId<0)this.id else newId, this.center.scale(1.0*this.count/(this.count+that.count)).sum(that.center.scale(1.0*that.count/ (this.count+that.count))), this.count+that.count, (this.clusterCost+that.clusterCost)/this.center.cosineSimilarity(that.center), scala.collection.mutable.ArrayBuffer[Int](), this.parentCenter, this.parentDiff, this.hierarchy, this.topWord, this.topWords, this.tags.filter(t => that.tags.contains(t)))
    def getDensity = this.clusterCost / this.count 
    def setParent(level:Int, id:Int) {
        if(this.parents.size==level-1) {
            this.parents.append(id)
        }
    }
    def setParentCenter(parentCenter:SemanticVector) {
        this.parentCenter = parentCenter
        this.parentDiff = this.center.semanticDiff(parentCenter)
    }
    def setHierarchy() =  {
        Cluster(this.id, this.center, this.count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, (this.parents.reverse ++ Seq(this.id)), this.topWord, this.topWords, this.tags)
    }
    def setHierarchy(hierarchy:Seq[Int]) =  {
        Cluster(this.id, this.center, this.count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, hierarchy, this.topWord, this.topWords, this.tags)
    }
    def setCount(count:Int) =  {
        Cluster(this.id, this.center, count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, this.hierarchy, this.topWord, this.topWords, this.tags)
    }
    def refineTopWordsAndName(size:Int = 10, corpusVector:SemanticVector) = {
        var keepGoing = true
        val reference = if(this.center.cosineSimilarity(corpusVector) > 0.99) SemanticVector("", corpusVector.coord.map(c => Coordinate(c.index, 1.0))) else corpusVector
        var phrase = this.topWords.take(size)//sortWith(_.semantic.relativeSimilarity(this.center, corpusVector) > _.semantic.relativeSimilarity(this.center, corpusVector)).take(size)
        var currentSim = phrase.map(p => p.semantic).reduce((v1, v2) => v1.sum(v2)).relativeSimilarity(this.center, reference)
        while(keepGoing) {
            keepGoing = false
            for(word<-this.topWords
                if(!phrase.map(p => p.root).contains(word.root))) 
            {
                var noUpdated = phrase
                for(i<-Range(0, size)) {
                    var updated = noUpdated.updated(i, word)
                    var updatedSim = updated.map(p => p.semantic).reduce((v1, v2) => v1.sum(v2)).relativeSimilarity(this.center, reference)
                    if(updatedSim > currentSim) {
                        phrase = updated
                        currentSim = updatedSim
                        keepGoing = true
                    }
                }
            }
        }
        var refined = if(this.parentDiff == null) phrase.sortWith(_.clusterCount > _.clusterCount) else phrase.sortWith(_.semantic.cosineSimilarity(this.center) > _.semantic.cosineSimilarity(this.center) )
        Cluster(this.id, this.center, this.count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, this.hierarchy, refined(0).root, refined, this.tags)
    }
    def setWord(wic:Word) = Cluster(this.id, this.center, this.count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, this.hierarchy, wic.root, Seq(wic), this.tags)
    def addWords(that:Cluster) = {
        val words = (this.topWords ++ that.topWords).sortWith(_.clusterCount > _.clusterCount)
        Cluster(this.id, this.center, this.count, this.clusterCost, this.parents, this.parentCenter, this.parentDiff, this.hierarchy, if(words.size > 0) words(0).root else null, words, this.tags)
    }
}; object Cluster {
    def distanceFromCenter(center:SemanticVector, point:SemanticVector) = 1-center.cosineSimilarity(point)

    def fromWordTreeRow(row:org.apache.spark.sql.Row):Seq[Cluster] = {
        import org.apache.spark.sql.Row;
        Seq(Cluster(
                row.getAs[Seq[Long]]("hierarchy").last.toInt
                ,SemanticVector.fromWordAndPairs(row.getAs[String]("name"), row.getAs[Seq[Row]]("semantic").map(r => (r.getAs[Long]("i").toInt, r.getAs[Double]("v"))).toVector)
                ,row.getAs[Long]("size").toInt
                ,row.getAs[String]("density").toDouble
                ,scala.collection.mutable.ArrayBuffer() ++ row.getAs[Seq[Long]]("hierarchy").map(i => i.toInt) match { case a => a.slice(0, a.size-1).reverse}
                ,null
                ,null
                ,row.getAs[Seq[Long]]("hierarchy").map(l => l.toInt)
                ,row.getAs[String]("name")
                ,null
        )) ++ row.getAs[Seq[Row]]("children").flatMap(r => Cluster.fromWordTreeRow(r))
    }
}
trait WithHierarchy {
    val hierarchy:Seq[Int]
    def setHierarchy(hierarchy:Seq[Int]):WithHierarchy
    def getId = hierarchy.last
    def getParentHierarchy = hierarchy.slice(0, hierarchy.size -1)
    def isAncestor(descendant:WithHierarchy) = descendant.hierarchy.size >= this.hierarchy.size && descendant.hierarchy.slice(0, this.hierarchy.size)== this.hierarchy
    def getAncestors() = Range(0, this.hierarchy.size).map(i => this.setHierarchy(hierarchy.slice(0, i+1)))
};trait WithParentHierarchy extends WithHierarchy {
    val parentHierarchy:Seq[Int]
    def setParentHierarchy(hierarchy:Seq[Int], parentHierarchy:Seq[Int])
    def derivateParent = this.setParentHierarchy(this.hierarchy, this.getParentHierarchy)
};trait WithCenterId {
    val centerId:Int
    def setCenterId(centerId:Int):WithCenterId
};trait WithCenterSemantic extends WithCenterId {
    val center:SemanticVector
    def setCenterSemantic(centerId:Int, center:SemanticVector):WithCenterSemantic
    def setCenterId(centerId:Int) = setCenterSemantic(centerId = centerId, center = this.center)
    def toEmptyCenterTagged() = CenterTagged(centerId = this.centerId, center = this.center, tags= Array[String]())
};trait WithCenterStats  extends WithCenterId {
    val docCount:Int
    val phraseCount:Int
    val totalDistance:Double
    def setCenterStats(centerId:Int, docCount:Int, phraseCount:Int, totalDistance:Double):WithCenterStats
    def setCenterId(centerId:Int) = setCenterStats(centerId = centerId, docCount = this.docCount, phraseCount = this.phraseCount, totalDistance = this.totalDistance)
    def mergeStatsWithinDoc(that:WithCenterStats) = setCenterStats(centerId = this.centerId, docCount = 1, phraseCount = this.phraseCount + that.phraseCount, totalDistance = this.totalDistance + that.totalDistance)
};trait WithCenterStatsByDoc extends WithCenterStats {
    val docId:Int
    def setCenterStatsByDoc(centerId:Int, docId:Int, docCount:Int, phraseCount:Int, totalDistance:Double):WithCenterStatsByDoc
    def setCenterStats(centerId:Int, docCount:Int, phraseCount:Int, totalDistance:Double) 
        = setCenterStatsByDoc(centerId = centerId, docId = this.docId, docCount = docCount, phraseCount = phraseCount, totalDistance = totalDistance)
    def mergeStatsBetweenDoc(that:CenterStats) 
        = setCenterStatsByDoc(centerId = this.centerId, docId = this.docId, docCount = this.docCount + that.docCount, phraseCount = this.phraseCount + that.phraseCount, totalDistance = this.totalDistance + that.totalDistance)
};trait WithCenterSemanticStat extends WithCenterSemantic {
    val phraseCount:Int
    def setCenterSemanticStat(centerId:Int, center:SemanticVector, phraseCount:Int):WithCenterSemanticStat
    def setCenterSemantic(centerId:Int, center:SemanticVector) = setCenterSemanticStat(centerId = centerId, center = center, phraseCount = this.phraseCount)
    def averageCenter(that:WithCenterSemanticStat) = setCenterSemanticStat(centerId = this.centerId, phraseCount = this.phraseCount + that.phraseCount
                                                                            , center = this.center.scale(1.0*this.phraseCount/(this.phraseCount+that.phraseCount))
                                                                                                .sum(that.center.scale(1.0*that.phraseCount/(this.phraseCount+that.phraseCount))))
};case class CenterSemanticStats(centerId:Int, center:SemanticVector, phraseCount:Int) extends WithCenterSemanticStat {
    def setCenterSemanticStat(centerId:Int, center:SemanticVector, phraseCount:Int) = CenterSemanticStats(centerId = centerId, center = center, phraseCount = phraseCount)
};case class CenterSemantic(centerId:Int, center:SemanticVector) extends WithCenterSemantic {
    def setCenterSemantic(centerId:Int, center:SemanticVector)=CenterSemantic(centerId = centerId, center = center)
};case class CenterStats(centerId:Int, docCount:Int, phraseCount:Int, totalDistance:Double) extends WithCenterStats {
    def setCenterStats(centerId:Int, docCount:Int, phraseCount:Int, totalDistance:Double) 
            = setCenterStats(centerId = centerId, docCount = docCount, phraseCount = phraseCount, totalDistance = totalDistance)
};case class CenterStatsByDoc(centerId:Int, docId:Int, docCount:Int, phraseCount:Int, totalDistance:Double) extends WithCenterStatsByDoc {
    def setCenterStatsByDoc(centerId:Int, docId:Int, docCount:Int, phraseCount:Int, totalDistance:Double) 
            = CenterStatsByDoc(centerId = centerId, docId = docId, docCount = docCount, phraseCount = phraseCount, totalDistance = totalDistance)
};trait WithCenterTagged extends WithCenterSemantic with WithTags {
    def setCenterTagged(centerId:Int, center:SemanticVector, tags: Seq[String]):WithCenterTagged
    def setTags(tags: Seq[String]) = this.setCenterTagged(centerId = this.centerId, center = this.center, tags = tags)
    def setCenterSemantic(centerId:Int, center:SemanticVector) = this.setCenterTagged(centerId = centerId, center = center, tags = this.tags)
    def addCenterWith(that:WithCenterTagged) = this.setCenterTagged(centerId = this.centerId, center = this.center.sum(that.center).scale(0.5), tags = (this.tags ++ that.tags).distinct)
    def toPhraseOnCluster() = PhraseOnCluster(docId = -1, phraseId = -1, semantic = center, clusterId = this.centerId, distance = 1, tags = tags)
    def toNodeTagged(hierarchy:Seq[Int]) = NodeTagged(centerId = centerId, hierarchy = hierarchy, center = this.center, tags = this.tags)
    def setOrigin(origin:Short) = CenterTaggedOrigin(centerId = this.centerId, center = this.center,tags = this.tags, origin = origin)
    
};case class CenterTagged(centerId:Int, center:SemanticVector,tags: Seq[String]) extends WithCenterTagged {
    def setCenterTagged(centerId:Int, center:SemanticVector, tags: Seq[String]) 
            = CenterTagged(centerId = centerId, center = center, tags = tags)

};object CenterTagged {
    def userContext2LeafClusters(userContextPath:Seq[String], userClustersPath:String, spark:org.apache.spark.sql.SparkSession) = {
        import spark.implicits._
        val ancest = 
            spark.read.option("multiline", true).json(userClustersPath)
                .flatMap(r => Cluster.fromWordTreeRow(r))
                .flatMap(c => c.getAncestors().map(a => (c, a.hierarchy)))

        val taggedHierarchy = TaggedHierarchy.userContext2Dataset(userContextPath, spark)

        ancest.joinWith(taggedHierarchy, ancest.col("_2")===taggedHierarchy.col("hierarchy"), "left")
            .flatMap(p => p match {case((cluster, anc), taggedHierarchy) => if(cluster.hierarchy.size == 10) 
                                                                                Some(CenterTagged(centerId = cluster.id, center = cluster.center, tags = if(taggedHierarchy==null) null else taggedHierarchy.tags)) 
                                                                            else None
            }).groupByKey(ct => ct.centerId)
            .reduceGroups((ct1, ct2) => CenterTagged(centerId = ct1.centerId, center = ct1.center, tags = ((if(ct1.tags==null) Seq[String]() else ct1.tags) ++ (if(ct2.tags==null) Seq[String]() else ct2.tags)).distinct))
            .map(p => p._2)

    }
};trait WithNodeTagged extends WithCenterTagged {
    val hierarchy:Seq[Int]
    def setNodeTagged(centerId:Int, hierarchy:Seq[Int], center:SemanticVector, tags: Seq[String]):WithCenterTagged
    def setCenterTagged(centerId:Int, center:SemanticVector, tags: Seq[String]) = setNodeTagged(centerId = centerId, hierarchy = this.hierarchy, center = center, tags = tags)
    def toCluster(count:Int, clusterCost:Double) = Cluster(id = this.centerId, center = this.center, hierarchy = this.hierarchy, count=count, clusterCost=clusterCost, tags = this.tags)
};case class NodeTagged(centerId:Int, hierarchy:Seq[Int], center:SemanticVector,tags: Seq[String]) extends WithNodeTagged {
    def setNodeTagged(centerId:Int, hierarchy:Seq[Int], center:SemanticVector, tags: Seq[String]) 
            = NodeTagged(centerId = centerId, hierarchy = hierarchy, center = center, tags = tags)
};trait WithCenterTaggedOrigin extends WithCenterTagged {
    val origin:Short
    def setCenterTaggedOrigin(centerId:Int, center:SemanticVector, tags: Seq[String], origin:Short):WithCenterTaggedOrigin
    def setCenterTagged(centerId:Int, center:SemanticVector, tags: Seq[String]) = setCenterTaggedOrigin(centerId = centerId, center = center, tags = tags, origin = this.origin)
};case class CenterTaggedOrigin(centerId:Int, center:SemanticVector,tags: Seq[String], origin:Short) extends WithCenterTaggedOrigin {
    def setCenterTaggedOrigin(centerId:Int, center:SemanticVector, tags: Seq[String], origin:Short) = CenterTaggedOrigin(centerId = centerId, center = center, tags = tags, origin = origin)
};object CenterTaggedOrigin {
    val cluster = 0.toShort
    val randomPhrase = 1.toShort
    val badTag = 2.toShort
    def getOrigin(v:Short) = (v match  {case 0 => "cluster" case 1 => "randomPhrase" case 2 => "badTag"} )
};trait WithClusterTaggedStats extends WithCenterTagged with WithCenterStats {
    val hierarchy:Seq[Int]
    def setClusterTaggedStats(centerId:Int, center:SemanticVector, tags: Seq[String], docCount:Int, phraseCount:Int, totalDistance:Double, hierarchy:Seq[Int]):WithClusterTaggedStats
    def setTags(centerId:Int, center:SemanticVector, tags: Seq[String]) =  setClusterTaggedStats(centerId =  this.centerId, center= this.center, tags = tags
                                        , docCount = this.docCount, phraseCount = this.phraseCount, totalDistance = this.totalDistance, hierarchy = this.hierarchy) 
    def setCenterTagged(centerId:Int, center:SemanticVector, tags: Seq[String]) = setClusterTaggedStats(centerId =  centerId, center= center, tags = tags
                                        , docCount = this.docCount, phraseCount = this.phraseCount, totalDistance = this.totalDistance, hierarchy = this.hierarchy) 
    def addCenterWith(that:WithClusterTaggedStats) = 
                    this.setClusterTaggedStats(centerId = this.centerId, center = this.center.scale(this.phraseCount).sum(that.center.scale(that.phraseCount)).scale(this.phraseCount + that.phraseCount)
                                                        , tags = (this.tags ++ that.tags).distinct
                                                        , docCount = this.docCount, phraseCount = this.phraseCount + that.phraseCount, totalDistance = this.totalDistance + that.totalDistance
                                                        , hierarchy = this.hierarchy)
    def setCenterStats(centerId: Int, docCount: Int, phraseCount: Int, totalDistance: Double) = setClusterTaggedStats(centerId = centerId, center=this.center, tags = this.tags
                                                        , docCount = docCount, phraseCount = phraseCount, totalDistance = totalDistance, hierarchy = this.hierarchy)
    override def setCenterId(centerId:Int) 
        = setClusterTaggedStats(centerId = centerId, center = this.center, tags = this.tags, docCount = this.docCount, phraseCount = this.phraseCount, totalDistance = this.totalDistance, hierarchy = this.hierarchy)

};case class ClusterTaggedStats(centerId:Int, center:SemanticVector,tags: Seq[String], docCount:Int, phraseCount:Int, totalDistance:Double, hierarchy:Seq[Int]) extends WithClusterTaggedStats {
    def setClusterTaggedStats(centerId:Int, center:SemanticVector, tags: Seq[String], docCount:Int, phraseCount:Int, totalDistance:Double, hierarchy:Seq[Int]) 
            = ClusterTaggedStats(centerId = centerId, center = center, tags = tags, docCount = docCount, phraseCount = phraseCount, totalDistance = totalDistance, hierarchy = hierarchy)
};object ClusterTaggedStats {
    def userContext2LeafTaggedClusters(userContextPath:Seq[String], userClustersPath:String, spark:org.apache.spark.sql.SparkSession) = {
        import spark.implicits._

        val ancest = 
            spark.read.option("multiline", true).json(userClustersPath)
                .flatMap(r => Cluster.fromWordTreeRow(r))
                .flatMap(c => c.getAncestors().map(a => (c, a.hierarchy)))

        val taggedHierarchy = TaggedHierarchy.userContext2Dataset(userContextPath, spark)

        ancest.joinWith(taggedHierarchy, ancest.col("_2")===taggedHierarchy.col("hierarchy"))
            .flatMap(p => p match {case((cluster, anc), taggedHierarchy) => if(cluster.hierarchy.size == 10) 
                                                                                Some(ClusterTaggedStats(centerId = cluster.id, hierarchy = cluster.hierarchy, center = cluster.center, tags = if(taggedHierarchy==null) null else taggedHierarchy.tags, phraseCount = cluster.count, totalDistance = cluster.clusterCost, docCount = 0 )) 
                                                                            else None
            }).groupByKey(ct => ct.centerId)
            .reduceGroups((ct1, ct2) => ClusterTaggedStats(centerId = ct1.centerId, center = ct1.center, tags = ((if(ct1.tags==null) Seq[String]() else ct1.tags) ++ (if(ct2.tags==null) Seq[String]() else ct2.tags)).distinct
                , phraseCount = ct1.phraseCount, docCount = ct1.phraseCount, totalDistance = ct1.totalDistance, hierarchy = ct1.hierarchy))
            .map(p => p._2)

    }
}
trait WithTaggedHierarchy extends WithHierarchy with WithTags{
    def setTagHierarchy(tags:Seq[String], hierarchy:Seq[Int]):WithTaggedHierarchy
    def setHierarchy(hierarchy:Seq[Int]) = setTagHierarchy(tags = this.tags, hierarchy = hierarchy)
    def setTags(tags:Seq[String]) = setTagHierarchy(tags = tags, hierarchy = this.hierarchy)
//    def toTaggedParentHierarchy() = TaggedParentHierarchy(tags = this.tags, hierarchy = this.hierarchy, parentHierarchy = this.getParentHierarchy)
};trait WithTaggedParentHierarchy extends WithParentHierarchy with WithTags{
    def setTagParentHierarchy(tags:Seq[String], hierarchy:Seq[Int], parentHierarchy:Seq[Int]):WithTaggedParentHierarchy
    def setParentHierarchy(hierarchy:Seq[Int], parentHierarchy:Seq[Int]) = setTagParentHierarchy(tags = this.tags, hierarchy = hierarchy, parentHierarchy = parentHierarchy) 
    def setTagHierarchy(tags:Seq[String], hierarchy:Seq[Int]) = setTagParentHierarchy(tags = tags, hierarchy = hierarchy, parentHierarchy = this.getParentHierarchy)
    def setHierarchy(hierarchy:Seq[Int]) = setTagParentHierarchy(tags = this.tags, hierarchy = hierarchy, parentHierarchy = this.getParentHierarchy)
    def setTags(tags:Seq[String]) = setTagParentHierarchy(tags = tags, hierarchy = this.hierarchy, parentHierarchy = this.parentHierarchy)
};case class TaggedHierarchy(tags:Seq[String], hierarchy:Seq[Int]) extends WithTaggedHierarchy {
   def setTagHierarchy(tags:Seq[String], hierarchy:Seq[Int]) = TaggedHierarchy(tags = tags, hierarchy = hierarchy)
};object TaggedHierarchy {
    def userContext2Dataset(userContextPath:Seq[String], spark:org.apache.spark.sql.SparkSession) = {
        import spark.implicits._
        val contextSTR = spark.sparkContext.textFile(userContextPath.last).collect.mkString("\n")
        val parsed = scala.util.parsing.json.JSON.parseFull(contextSTR)
        val nomatch = Seq[TaggedHierarchy]()
        val taggedNodes = parsed match {
            case Some(map:Map[_,_]) => map.asInstanceOf[Map[String, Any]]("taggedNodes") match {
                case tn:Map[_,_] => {
                    val nodeTags = tn.asInstanceOf[Map[String, List[String]]]
                    nodeTags.toSeq.map(p => p match { case (clusterId, tags) =>
                                            TaggedHierarchy(hierarchy = clusterId.split(",").map(s => s.toInt).toSeq ,tags = tags.toSeq)
                                        })
                }
                case _ => nomatch
            }
            case _ => nomatch
        }
      spark.sparkContext.parallelize(taggedNodes).toDS
    }
};case class TaggedParentHierarchy(tags:Seq[String], hierarchy:Seq[Int], parentHierarchy:Seq[Int]) extends WithTaggedParentHierarchy {
   def setTagParentHierarchy(tags:Seq[String], hierarchy:Seq[Int], parentHierarchy:Seq[Int]) = TaggedParentHierarchy(tags = tags, hierarchy = hierarchy, parentHierarchy = parentHierarchy) 
}

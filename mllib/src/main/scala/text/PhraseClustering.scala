package demy.mllib.text

import demy.mllib.linalg.SemanticVector
import demy.mllib.linalg.Coordinate
import demy.mllib.util.util
import demy.mllib.util.log
import java.nio.file.{Paths, Files}
import java.nio.file.{Paths, Files, StandardOpenOption}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.storage.StorageLevel._
import scala.collection.JavaConverters._
import scala.util.Random

case class PhraseClustering(numClusters:Int, taggedCentersPath:String,userContextPath:Seq[String], userClustersPath:String, phraseVectorsPath:String
                             , phraseClustersPath:String, phraseHierarchy:String, hirarchicalClusters:String ,clusterInfoPath:String, clusterOutputPath:String
                             , phrasesOutputPath:String, contextOutputPath:String, spark:org.apache.spark.sql.SparkSession) {
    def applyClustering(keepExistingIteration:Boolean=false, reusePreviousIterationCenters:Boolean= false, ignoreTags:Set[String] = Set[String]()
                          , minIterations:Int = -1, maxIterations:Int = -1) = {
        //Modified k-means on phrases
        import spark.implicits._

        val phrases = spark.read.parquet(this.phraseVectorsPath).select($"docId", $"phraseId", $"phraseSemantic".as("semantic")).as[PhraseSemantic]
        var previousTaggedClusters = CenterTagged.userContext2LeafClusters(this.userContextPath, userClustersPath, spark)
                .map(c => CenterTagged(centerId = c.centerId, center = c.center, tags = c.tags.toSet.diff(ignoreTags).toSeq.slice(0,1)))
        val previousTaggedPhrases = util.checkpoint(
                TaggedPhrase.userContext2Dataset(this.userContextPath, spark)
                        .joinWith(phrases, $"_1.docId"===$"_2.docId" && $"_1.phraseId"===$"_2.phraseId")
                        .map(p => p match { case(taggedPhrase, phraseSem) => CenterTagged(centerId = -1, center = phraseSem.semantic, tags=taggedPhrase.isTag)})
                        .map(c => CenterTagged(centerId = c.centerId, center = c.center, tags = c.tags.toSet.diff(ignoreTags).toSeq))
                        .filter(c => c.tags.size > 0)
            ,"hdfs:///tmp/previousTaggedPhrases")                    

        //Adding previous tagged clusters if we have tagged phrases without a cluster
        val orphelinTags = previousTaggedPhrases.flatMap(c => c.tags).distinct.joinWith(previousTaggedClusters.flatMap(c => c.tags).distinct, $"_1" === $"_2", "left").filter(p => p._2 == null).map(p => p._1).collect
        if(orphelinTags.size > 0) {
            log.msg(s"Found (${orphelinTags.mkString(", ")} orphehelin tags, they will be added as new centers")
            val maxCluster = previousTaggedClusters.map(c => c.centerId).reduce((a, b)=> if(a>b) a else b)
            val newCenters = previousTaggedPhrases.flatMap(c => c.tags.map(t => CenterTagged(centerId = -1, center = c.center, tags=Seq(t))))
                                .groupByKey(c => c.tags(0))
                                .reduceGroups((c1, c2) => c1.addCenterWith(c2).asInstanceOf[CenterTagged])
                                .map(p => p._2)
                                .withColumn("centerId", lit(maxCluster) + dense_rank().over(Window.orderBy($"tags".asc))).as[CenterTagged]
            //newCenters.show
            previousTaggedClusters = previousTaggedClusters.union(newCenters)
            log.msg("New orphelin centers added")
        }
        //Finding best phrases to start iteration bases on previous clusters
        log.msg("Finding best center phrases by tagged cluster")
        val previousTaggedClustersCollected = previousTaggedClusters.filter(c => c.tags.size>0).collect
        val bestTagPhrases = util.checkpoint(
            phrases
            .joinWith(TaggedPhrase.userContext2Dataset(this.userContextPath, spark), $"_1.docId"===$"_2.docId" && $"_1.phraseId"===$"_2.phraseId", "left") //validating if this phrase has been manually tagged on an incompatible way with the cluster
            .flatMap(p => p match {case (phrase, taggedPhrase) => { 
                val inScopeCenters = previousTaggedClustersCollected.flatMap(center => {
                                                            if(taggedPhrase != null && (taggedPhrase.isTag.toSet.diff(center.tags.toSet).size > 0 ||  center.tags.toSet.intersect(taggedPhrase.isNotTag.toSet).size > 0))
                                                                None
                                                            else
                                                                Some(phrase.setCenter(center = center, tags = center.tags))
                                                })  
                if(inScopeCenters.size == 0)
                    None
                else 
                    Some(inScopeCenters.reduce((p1, p2) => if(p1.distance < p2.distance) p1 else p2))
            }})
            .groupByKey(p => p.clusterId)
            .reduceGroups((p1, p2) => if(p1.distance < p2.distance) p1 else p2).map(p => p._2)
            .map(p => p.toCenterTagged(keepId = true))
            , "hdfs:///tmp/bestTagPhrases")
        val bestTaggedCount = bestTagPhrases.count.toInt
        log.msg(s"found $bestTaggedCount cluster representants")

        //Counting how many phrases has been manually tagged by tag so we can adjust simlple phrase gravity
        log.msg("Counting phrases by TAG")
        val tagPhrasesCounts = previousTaggedPhrases.flatMap(p => p.tags.map(t => (t, 1)))
                .groupByKey(p => p match {case (tag, count) => tag})
                .reduceGroups((p1, p2) => (p1, p2) match {case ((tag1, count1), (tag2, count2)) => (tag1, count1 + count2)})
                .map(p => p._2)
                .collect.toMap
        log.msg(tagPhrasesCounts)
        val estTotal = phrases.count.toInt 
        
        var taggedPhrases:org.apache.spark.sql.Dataset[PhraseOnCluster] = null
        var taggedClusters:org.apache.spark.sql.Dataset[CenterTaggedOrigin] = null
        if(keepExistingIteration) {
            taggedPhrases =  spark.read.parquet(this.phraseClustersPath).as[PhraseOnCluster]
            taggedClusters = spark.read.parquet(this.taggedCentersPath).as[CenterTaggedOrigin]           
        }

        var iterating = false
        var finish = false
        var oldCost = Double.MaxValue
        var oldPhrasesCount = 1
        var iterations = 0
        while(!finish) {
            //1. Getting initial centers
            print("Iteration Start: ")
            val iterationStart = (
              if(iterating || keepExistingIteration) {
                 log.msg("Using previous iteration phrases, recalculating center")
                 /*val taggedCenters = spark.read.parquet(taggedCentersPath).as[CenterTagged]*/
                 /*spark.read.parquet(this.phraseClustersPath).where($"semantic".isNotNull).as[PhraseOnCluster]*/
                 taggedPhrases
                      .map(p => p.toCenterSemanticStats())
                      .groupByKey(centerStatByDoc => (centerStatByDoc.centerId))
                      .reduceGroups((s1, s2) => s1.averageCenter(s2).asInstanceOf[CenterSemanticStats])
                      .map(p => p._2)
                      .joinWith(taggedClusters, $"_1.centerId"===$"_2.centerId")
                      .map(p => p match {case (newCenter, oldCenter) => CenterTagged(centerId = -2, center = newCenter.center, tags=oldCenter.tags).setOrigin( origin = CenterTaggedOrigin.cluster)})
              }
              else if(reusePreviousIterationCenters){
                  log.msg(s"Using tags from previous iteration")
                  previousTaggedClusters.map(c => c.setOrigin(origin = CenterTaggedOrigin.cluster))
              } else {
                  log.msg(s"Using phrases from $bestTaggedCount existing tagged  clusters and random for the rest")
                  bestTagPhrases.map(c => CenterTaggedOrigin(center = c.center, centerId = -2, tags = c.tags, origin = CenterTaggedOrigin.cluster))
              })
            //2 Adding more centers as needed to fill the expected count
            val randomCenters = spark.read.parquet(this.phraseVectorsPath).where($"phraseSemantic".isNotNull).select(lit(-1).as("centerId"), $"phraseSemantic".as("center")).as[CenterSemantic]
                .map(c => c.toEmptyCenterTagged().setOrigin(origin = CenterTaggedOrigin.randomPhrase))
                .sample(false, (2.0*numClusters)/estTotal)

            val baseCenters = util.checkpoint(
                        iterationStart.union(randomCenters)
                            .map(c => (c, c.center.semanticHash)).toDF("center", "semId")
                            .withColumn("rank", rank().over(Window.partitionBy($"semId").orderBy($"center.centerId".asc)))
                            .where($"rank" === 1).select($"center.*")
                            .orderBy($"centerId".asc)
                            .withColumn("centerId", row_number().over(Window.orderBy($"centerId".asc))).as[CenterTaggedOrigin]
                            .limit(numClusters)
                    , "hdfs:///tmp/baseCenters")
            import spark.implicits._
            log.msg(s"${baseCenters.count} base clusters found on ${baseCenters.rdd.getNumPartitions} partitions")
            log.msg(s"${baseCenters.map(c => c.center.coord).distinct.count} vectors >> ${baseCenters.count} vectors")
/*            if(taggedPhrases!= null) {
                taggedPhrases.unpersist
                taggedClusters.unpersist
            }*/
            //println(s"origin type >> ${baseCenters.map(c => (c.origin, 1)).groupByKey(p => p._1).reduceGroups((p1, p2)=>(p1._1, p1._2+p2._2)).map(p => p._2).collect.mkString("||")}")             //3 reassigning tags tu current clusters
            log.msg(s"reassigning tags tu current clusters based on manual tagged phrases cluster")
            var collectedCenters = baseCenters.collect()
            val newTags = previousTaggedClusters
                                .map(taggedP => collectedCenters.map(c => c.toPhraseOnCluster().setCenter(center = taggedP.setCenterSemantic(centerId=c.centerId, center = taggedP.center) , tags = taggedP.tags))
                                                                .reduce((ph1, ph2) => if(ph1.distance < ph2.distance) ph1 else ph2)
                                )
                                .map(p => p.toCenterTagged(keepId = true))
            val reestimatedCenterTags = util.checkpoint(
                    baseCenters.joinWith(newTags, baseCenters("centerId")===newTags("centerId"), "left")
                                .map(p => p match { case (center, newTags) => center.setTags(if(newTags==null) Seq[String]() else newTags.tags).asInstanceOf[CenterTaggedOrigin]})
                                .groupByKey(c => c.centerId)
                                .reduceGroups((c1, c2) => c1.setTags(c1.tags.toSet.union(c2.tags.toSet).toSeq.slice(0,1)).asInstanceOf[CenterTaggedOrigin])
                                .map(p => p._2)
                    ,"hdfs:///tmp/restimatedCenterTags")
                            
            collectedCenters = reestimatedCenterTags.collect

            //Identifying manually tagged phrases that are closest to a center having a different tag
            val badTags = util.checkpoint(
                previousTaggedPhrases.map(c => c.toPhraseOnCluster()) /*previousTaggedPhrases*/
                    .flatMap(prevTag => {
                            val inScopeCenters = collectedCenters.map(center => prevTag.setCenter(center, center.tags))
                            val bestMatch = inScopeCenters.reduce((ph1, ph2) => if(ph1.distance < ph2.distance) ph1 else ph2)
                            if(bestMatch.tags.filter(bt => !prevTag.tags.contains(bt)).size > 0)
                                Some(prevTag.toCenterTagged(keepId = false))
                            else None
                    })
                    .map(c => c.setOrigin(origin = CenterTaggedOrigin.badTag))
                ,"hdfs:///tmp/badTags")
            log.msg(s"We have found ${badTags.count} improperly tagged phrases/clusters adding them as forced clusters on ${badTags.rdd.getNumPartitions} partitions")
                

            //3. Associating phrases to new centers
            val iterationCenters = reestimatedCenterTags
                    .joinWith(badTags,  $"_1.center.coord"===$"_2.center.coord", "full")
                    .map(p => p match { case (center, badTag) => if(badTag != null && center != null) badTag.setCenterId(center.centerId).asInstanceOf[CenterTaggedOrigin] else if(badTag != null) badTag else center})
            collectedCenters = iterationCenters.collect
            log.msg(s"${collectedCenters.size} fixed clusters")
            val phrasesOnCenter = util.checkpoint(
                phrases.filter(p => p.semantic!=null && p.semantic.coord.size>0)
                    .map(phrase => {
                            val theCenter = collectedCenters.reduce((c1, c2) => {
                                //val ph1 = phrase.setCenter(c1, c1.tags)
                           //val ph2 = phrase.setCenter(c2, c2.tags)
                                val weight1 = (if(c1.tags.size == 0 || c1.origin != CenterTaggedOrigin.badTag) 1 else c1.tags.map(t => if(tagPhrasesCounts.contains(t)) tagPhrasesCounts(t) else 1).reduce((w1, w2)=> w1 + w2))
                                val weight2 = (if(c2.tags.size == 0 || c2.origin != CenterTaggedOrigin.badTag) 1 else c2.tags.map(t => if(tagPhrasesCounts.contains(t)) tagPhrasesCounts(t) else 1).reduce((w1, w2)=> w1 + w2))
                                val distance1 = phrase.semantic.distanceWith(c1.center)* Math.pow(weight1, 0.5)/Math.pow(weight1, 0.4) //ph1.distance 
                                val distance2 = phrase.semantic.distanceWith(c2.center) * Math.pow(weight2, 0.5)/Math.pow(weight2, 0.4) //ph2.distance
                                if(distance1 < distance2) c1 else c2
                            })
                            phrase.setCenter(theCenter, theCenter.tags)
                    })
                ,"hdfs:///tmp/phrasesOnCenter")
            val missingClusters = (numClusters - phrasesOnCenter.flatMap(p => if(p.clusterId == -1)None else Some(p.clusterId)).distinct.count).toInt
            log.msg(s"Centers loosed =${missingClusters} on  on ${phrasesOnCenter.rdd.getNumPartitions} partitions")
            
            //4. Calculating new cost (excluding forced phrases)
            log.msg(" Calculating new cost (excluding forced phrases)")
            val stat = phrasesOnCenter.filter(p => p.clusterId>=0).map(phraseCenter => phraseCenter.toCenterStatsByDoc())
                .reduce((cs1, cs2) => cs1.mergeStatsWithinDoc(cs2).asInstanceOf[CenterStatsByDoc])
            //6. we will associate the closest real center with phrases that were associated to manual tagged phrases
            val phrasesMoved =  util.checkpoint(
                phrasesOnCenter.filter(p => p.clusterId == -1)
                    .flatMap(phrase => {
                            val inScopeCenters = collectedCenters.filter(c => c.centerId >= 0).flatMap(center => 
                                    if(center.tags.size == 0 && phrase.tags.size == 0) Some(phrase.setCenter(center)) //a phrase marked to belong to no tag will go to a cluster wothout tag
                                    else if(phrase.tags.filter(t => center.tags.contains(t)).size > 0) Some(phrase.setCenter(center)) //a phrase with tags will go to the closest center having one of their tags
                                    else None)
                            if(inScopeCenters.size > 0)
                                Some(inScopeCenters.reduce((ph1, ph2) => if(ph1.distance < ph2.distance) ph1 else ph2))
                            else {
                                val relaxedScopeCenters = collectedCenters.filter(c => c.centerId >= 0).flatMap(center => 
                                        if(center.tags.size == 0) Some(phrase.setCenter(center)) //a phrase with tags will go to the closest center not having a tag
                                        else None)
                                if(relaxedScopeCenters.size > 0)
                                    Some(relaxedScopeCenters.reduce((ph1, ph2) => if(ph1.distance < ph2.distance) ph1 else ph2))
                                else None
                            }
                    })
                ,"hdfs:///tmp/phrasesMoved")
                
            val movedCount = phrasesMoved.filter(p => p.clusterId >= 0).count
            log.msg(s" ${movedCount} phrases moved to other clusters! ")
                
            taggedPhrases =  util.checkpoint(phrasesOnCenter.filter(p => p.clusterId >= 0).union(phrasesMoved.filter(p => p.clusterId >= 0)), this.phraseClustersPath)//.union(phrasesInNewClusters)
            taggedClusters = util.checkpoint(iterationCenters.filter(c => c.centerId >=0)/*.union(iterationCenters.filter(c => c.centerId <0).limit(missingClusters))*/, this.taggedCentersPath)

            //7. Choosing if keep iterating or not
            if(   (oldCost/oldPhrasesCount <= stat.totalDistance/stat.phraseCount && (minIterations < 0 || iterations >=minIterations)) 
               || (maxIterations >= 0 && iterations >= maxIterations)
              ) {
                finish = true
                log.msg(s"Final cost is ${stat.totalDistance} for ${stat.phraseCount} phrases")
                log.msg("Writing tagged clusters")
                //taggedClusters.write.mode("Overwrite").parquet(this.taggedCentersPath)
                log.msg("Writing tagged phrases")
                //taggedPhrases.write.mode("Overwrite").parquet(this.phraseClustersPath)
            }
            else {
                iterating = true
                log.msg(s"Cluster cost when from $oldCost to ${stat.totalDistance} for ${stat.phraseCount} phrases")
                log.msg(s"${taggedPhrases.count} phrases cached")
                log.msg(s"${taggedClusters.count} clusters cached")
            }

            oldCost = stat.totalDistance
            oldPhrasesCount = stat.phraseCount
/*
            phrasesOnCenter.unpersist
            phrasesMoved.unpersist
            badTags.unpersist
            iterationCenters.unpersist
            baseCenters.unpersist
            reestimatedCenterTags.unpersist
            */
            iterations = iterations + 1
        }
        /*previousTaggedClusters.unpersist 
        previousTaggedPhrases.unpersist
        bestTagPhrases.unpersist
        taggedPhrases.unpersist
        taggedClusters.unpersist*/
    }
    def writeHierarchy() {
        import spark.implicits._

        val calculatedClusters = spark.read.parquet(this.taggedCentersPath).as[CenterTagged].collect
        val hBuilder = HierarchyBuilder(calculatedClusters)
        val allNodes = spark.sparkContext.parallelize(hBuilder.buildHierarchy().toSeq).toDS
        val clusteredPhrases = spark.read.parquet(this.phraseClustersPath).as[PhraseOnCluster]
        
        //Assingning hierarchy to phrases 
        clusteredPhrases
                .joinWith(allNodes, $"_1.clusterId"===$"_2.centerId")
                .map(p => p match { case (phrase, node) => phrase.toPhraseSemantic(node.hierarchy)})
                .write.mode("Overwrite").parquet(this.phraseHierarchy)
        val phrasesCluster = spark.read.parquet(this.phraseHierarchy).as[SemanticPhrase]


        //Calculating doc count per cluster
        phrasesCluster
            .map(p =>  (p.docId, p.hierarchy, p.phraseSemantic))
            .flatMap(t => t match { case (docId, hierarchy, phraseSemantic) => Range(0, hierarchy.size).map(level => (docId, hierarchy.slice(0, level+1), phraseSemantic)) })
                .toDF("docId", "hierarchy", "phraseSemantic").as[(Int, Seq[Int], Option[SemanticVector])]
            .joinWith(allNodes, $"_1.hierarchy"===$"_2.hierarchy")
            .map(p => p match {case ((docId, hierarchy, phraseSemantic), node) => (docId, hierarchy, 1, node.center.distanceWith(phraseSemantic.getOrElse(node.center)))})
            .groupByKey(q => q match { case (docId, hierarchy, phraseCount, phraseCost) => (docId, hierarchy)})
            .reduceGroups((q1, q2) => (q1, q2) match {case ((doc1, hierarchy1, phraseCount1, phraseCost1), (doc2, hierarchy2, phraseCount2, phraseCost2))=>(doc1, hierarchy1, phraseCount1 + phraseCount2, phraseCost1 + phraseCost2)})
            .map(p => p._2 match { case (docId, hierarchy, phraseCount, phraseCost) =>   (hierarchy, phraseCount, phraseCost, 1)})
            .groupByKey(q => q match {case (hierarchy, phraseCount, phraseCost, docCount) => hierarchy})
            .reduceGroups((q1, q2) => (q1, q2) match { case ((hierarchy1, phraseCount1, phraseCost1, docCount1),(hierarchy2, phraseCount2, phraseCost2, docCount2)) 
                                                            => (hierarchy1, phraseCount1+phraseCount2, phraseCost1+phraseCost2, docCount1+docCount2) })
            .map(p => p._2).toDF("hierarchy", "phraseCount", "clusterCost", "docCount").as[(Seq[Int], Int, Double, Int)]
            .joinWith(allNodes, $"_1.hierarchy"===$"_2.hierarchy")
            .map(p => p match {case ((hierarchy, phraseCount, clusterCost, docCount), node) => node.toCluster(docCount, clusterCost)})
            .write.mode("Overwrite").parquet(this.hirarchicalClusters)
        println("Final Clusters Written")
    }
    def checkClusterQuality() {
        import spark.implicits._

        val taggedPhrases = spark.read.parquet(phraseClustersPath).as[PhraseOnCluster]
        val forcedTags = TaggedPhrase.userContext2Dataset(userContextPath, spark)
        val PhrasetaggedVsForced = forcedTags.joinWith(taggedPhrases, $"_1.docId" === $"_2.docId" && $"_1.phraseId" === $"_2.phraseId")
        val allForcedPresent = PhrasetaggedVsForced.filter(p => p match {case (forced, tagged)=> tagged == null}).count  == 0
        println(s"All forced phrase present on tagged phrases: $allForcedPresent")
        val badTags = PhrasetaggedVsForced.filter(p => p match {case (forced, tagged)=> forced.isTag.size > 0 && forced.isTag.toSet.intersect(tagged.tags.toSet).size==0})
        val badTagsCount = badTags.count
        println(s"All forced phrases keep thair tags: ${badTagsCount == 0}, $badTagsCount bad tgged phrases found")
        val allForcedTagsAppliedOnPhrase = badTags.map(p => p match {case (forced, tagged)=> (tagged.tags, forced.isTag, forced.isNotTag)})
        val noManualCentersOnPhrases = taggedPhrases.filter(p => p.clusterId == -1).count == 0 
        println(s"Temporary centers removed: $noManualCentersOnPhrases")
        val clusters =  spark.read.parquet(taggedCentersPath).as[CenterTagged]
        val phrasesTaggesVsCenters = taggedPhrases.joinWith(clusters, $"_1.clusterId"===$"_2.centerId", "left")
        val allPhrasesOnCenter = phrasesTaggesVsCenters.filter(p => p match {case (phrase, center) => center == null}).count == 0
        println(s"All phrases has a center: $allPhrasesOnCenter")
        val PhraseTagsVsCLuster = phrasesTaggesVsCenters.filter(p => p match {case (phrase, center) => phrase.tags.toSet != center.tags.toSet}).count == 0
        println(s"All phrases tags in line with Cluster tag: $PhraseTagsVsCLuster")

        //calculating cluster quality
        val taggedCenters = spark.read.parquet(taggedCentersPath).as[CenterTagged]
        val taggedLeafClusters =CenterTagged.userContext2LeafClusters(userContextPath, userClustersPath, spark)
        
        val collectedCenters = taggedCenters.collect
        
        val retaggedClusters = 
            taggedLeafClusters
                .map(previousCluster => {
                        val inScopeCenters = collectedCenters.map(center => (1 - previousCluster.center.cosineSimilarity(center.center), center.tags, previousCluster.tags))
                        inScopeCenters.reduce((p1, p2) => if(p1._1 < p2._1) p1 else p2)
                })
                
        val clusterScore = retaggedClusters.map(t => t match {case (similarity, currentTags, previousTags) => (
            if(previousTags == null || previousTags.size == 0)
                ClusteringTagQuality(previousClusterTags = 1, maintainedClusterTags = 1)
            else
                ClusteringTagQuality(previousClusterTags = previousTags.size, maintainedClusterTags = currentTags.filter(c => previousTags.contains(c)).size)
            )}).reduce((p1, p2) => p1.reduceWith(p2))
        
        //calculating phrase quality
        val phrases = spark.read.parquet(phraseVectorsPath).select($"docId", $"phraseId", $"phraseSemantic".as("semantic")).as[PhraseSemantic].where($"semantic".isNotNull)
        val retaggedPhrases = 
            forcedTags.joinWith(phrases, $"_1.docId"===$"_2.docId" && $"_1.phraseId"===$"_2.phraseId")
                .map(p => p match {case (tagged, phrase) => SemanticTagPhrase(docId = phrase.docId, phraseId= phrase.docId, isTag = tagged.isTag, isNotTag = tagged.isNotTag, semantic = phrase.semantic)})
                .filter(p => p.semantic!=null)
                .map(phrase => {
                        val inScopeCenters = collectedCenters.map(center => (1 - phrase.semantic.cosineSimilarity(center.center), center.tags, phrase.isTag, phrase.isNotTag))
                        inScopeCenters.reduce((p1, p2) => if(p1._1 < p2._1) p1 else p2)
                })

        val phraseScore = retaggedPhrases.map(t => t match {case (similarity, clusterTags, phraseIsTag, phraseIsNotTag) => {
            val hasTag = phraseIsTag != null && phraseIsTag.size > 0
            val hasNotTag = phraseIsNotTag != null && phraseIsNotTag.size > 0
            val honoringNotTag = hasNotTag && phraseIsTag.filter(t => clusterTags.contains(t)).size==0
            val honoringTag = hasTag && phraseIsTag.filter(t => clusterTags.contains(t)).size>0
            ClusteringTagQuality(
                previousPhrasesInTags = if(hasTag) 1 else 0
                , maintainedPhraseInTags = if(honoringTag) 1 else 0
                , previousPhrasesOutTags = if(hasNotTag) 1 else 0
                , maintainedPhraseOutTags = if(honoringNotTag) 1 else 0
            )
        }}).reduce((p1, p2) => p1.reduceWith(p2))
        
        val result = clusterScore.reduceWith(phraseScore)
        println(result.toMessage)
    }
    def setClusterTopWords(wordTypeInScope:Seq[String], stopWords:Seq[String]) = {
        import spark.implicits._

        val phraseSemantic = spark.read.parquet(phraseVectorsPath).as[SemanticPhrase]
        val phrasesCluster = spark.read.parquet(phraseHierarchy).as[SemanticPhrase]
                .joinWith(phraseSemantic, $"_1.docId"===$"_2.docId" && $"_1.phraseId"===$"_2.phraseId")
                .map(p => p match {case (pCluster, pWords) => SemanticPhrase(docId = pWords.docId, phraseId= pWords.phraseId, phraseSemantic = pWords.phraseSemantic
                                                                                            ,  words = pWords.words.filter(w => w.semantic!=null && !stopWords.contains(w.root) 
                                                                                                                           && w.root.size >2 && wordTypeInScope.filter(wt => w.wordType == wt).size>0)
                                                                                            , clusterId = pCluster.clusterId, centerDistance = pCluster.centerDistance, hierarchy = pCluster.hierarchy)})
        val clusters = spark.read.parquet(this.hirarchicalClusters).as[Cluster].rdd.toDS
        val corpusVector = clusters.filter(c => c.hierarchy.size == 1).head.center
        phrasesCluster
            .flatMap(p =>  p.words.map(w => (w, p.hierarchy)))
            .flatMap(p => p match { case (word, hierarchy) => Range(0, hierarchy.size).map(level => (word, hierarchy.slice(0, level+1), 1)) })
            .groupByKey(t => t match {case (word, hierarchy, count) => (word.root, hierarchy)})
            .reduceGroups((t1, t2) => (t1, t2) match { case ((word1, hierarchy1, coun1),(word2, hierarchy2, count2)) => (word1, hierarchy1, coun1+count2) })
            .map(p => p._2).toDF("word", "hierarchy", "count")
            .withColumn("rn", row_number().over(Window.partitionBy($"hierarchy").orderBy($"count".desc))).where($"rn"<=100).select($"word", $"hierarchy").as[(Word, Seq[Int])]
            .joinWith(clusters, $"_1.hierarchy" === $"_2.hierarchy")
            .map(p => p match { case ((word, hierarchy), cluster) => cluster.setWord(word)})
            .groupByKey(c => c.hierarchy)
            .reduceGroups((c1, c2) => c1.addWords(c2))
            .map(p => p._2.refineTopWordsAndName(5, corpusVector))
            .coalesce(10)
            .write.mode("Overwrite").parquet(this.clusterInfoPath)
        
    }
    def writeOutput(clusters:Boolean = true, phrases:Boolean = true, context:Boolean = true, phrasesChinkSize:Int = 500) {
        import spark.implicits._
        
        if(clusters) {
            println("writing clusters output file")
            val root = spark.read.parquet(this.clusterInfoPath).as[Cluster].orderBy(size($"hierarchy")).first
            
            val tree = WordTree(Seq[Int](root.hierarchy(0)), Seq[String](), Seq[WordTree](), root.count, "", 0.0, root.center)
            val cInfos = spark.read.parquet(clusterInfoPath).as[Cluster].orderBy(size($"hierarchy"), $"hierarchy", $"count".desc).toLocalIterator()
            
            while(cInfos.hasNext) {
                val cInfo = cInfos.next
                tree.addNode(cInfo.hierarchy, cInfo.topWords.map(w => w.root), cInfo.count, cInfo.topWord, cInfo.getDensity, cInfo.center)
            }
            Files.write(Paths.get(clusterOutputPath), tree.toJson().getBytes(StandardCharsets.UTF_8))
        }
        if(phrases) {
            println("writing phrases output files")
            //val comparableHierarchies = org.apache.spark.sql.functions.udf((h1:Seq[Int], h2:Seq[Int]) => h1.slice(0, if(h1.size < h2.size) h1.size else h2.size) == h2.slice(0, if(h1.size < h2.size) h1.size else h2.size))
            val comp = org.apache.spark.sql.functions.udf((h:String) => h.split(",").map(s => s.toInt))
            val phraseSemantic = spark.read.parquet(this.phraseVectorsPath).as[SemanticPhrase]
            val phrases = spark.read.parquet(this.phraseHierarchy).as[SemanticPhrase]
                .joinWith(phraseSemantic, $"_1.docId"===$"_2.docId" && $"_1.phraseId"===$"_2.phraseId")
                .map(p => p match {case (pCluster, pWords) => SemanticPhrase(docId = pWords.docId, phraseId= pWords.phraseId, phraseSemantic = pWords.phraseSemantic
                                                                                            ,  words = pWords.words
                                                                                            , clusterId = pCluster.clusterId, centerDistance = pCluster.centerDistance, hierarchy = pCluster.hierarchy)})
            
            val clusters = spark.read.parquet(this.clusterInfoPath).as[Cluster].rdd.toDS
            
            val docs = phrases.map(p => (p.docId, Seq((p.phraseId, p.words.map(w => w.word.replaceAll("[\\\\\n\\\"]", " ")).mkString("")))))
                            .groupByKey(t => t match{ case (docId, phrases) => docId})
                            .reduceGroups((t1, t2) => (t1, t2) match {case ((docId1, phrases1), (docId2, phrases2)) 
                                                                => (docId1, (phrases1 ++ phrases2).sortWith(_._1 < _._1) )})
                            .map(p => p._2)
            //docs.take(10).foreach(p => {println(p._1); p._2.foreach(w => println(s"----->${w._2}"))})
        
            val pDistance0 = phrases.map(p => (p.phraseId, p.docId, p.phraseSemantic, p.hierarchy))
                                    .flatMap(p => p match { case (phraseId, docId, semantic, hierarchy) 
                                                    =>  Range(0, hierarchy.size).map(level => (phraseId, docId, semantic, hierarchy.slice(0, level + 1), hierarchy))
                                    })
            val pDistance = pDistance0
                                    .joinWith(clusters, pDistance0.col("_4") ===  clusters.col("hierarchy"))
                                    .map(p => p match { case ((phraseId, docId, semantic, hierarchy, fullHierarchy),cluster) 
                                                => (phraseId, docId, hierarchy, fullHierarchy ,semantic match { case Some(point) => Some(Cluster.distanceFromCenter(cluster.center, point)) case _ => None})})
            
            val phrasesOnCluster = pDistance.joinWith(docs, pDistance("_2")===docs("_1") /*docId*/)
                            .map(p => p match { case ((phraseId, pDocId, hierarchy,fullHierarchy, distance), (dDocId, phrases)) => (pDocId, phraseId, hierarchy, fullHierarchy, distance, phrases)})
                            .orderBy($"_3", $"_5")
                            .toLocalIterator()

            val destinationFolder = this.phrasesOutputPath
            val destFoder = new java.io.File(destinationFolder)
            destFoder.mkdir();
            destFoder.listFiles.foreach(f => f.delete)
        
            val fileIndex = s"$destinationFolder/index.json"
            Files.write(Paths.get(fileIndex), "{\n".getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            
            var pHierarchyId = ""
            var split = 0
            var line = 0
            val splitAt = phrasesChinkSize
            var newFile = true
            var firstFile = true
            var prevDest = ""
            var clusterChanged = true
        
            while(phrasesOnCluster.hasNext) { phrasesOnCluster.next match {
                case (docId, phraseId, hierarchy, fullHierarchy, distance, phrases) => {
                    val hierarchyId = hierarchy.mkString("-")
                    if(hierarchyId != pHierarchyId) {
                        newFile = true
                  line = 0
                        split = 0
                        clusterChanged = true
                    }
                    else if(line >= splitAt) {
                        split = split + 1
                        line = 0
                        newFile = true
                        clusterChanged = false
                    } else {
                        newFile = false
                        line = line + 1
                        clusterChanged = false
                    }
                    pHierarchyId = hierarchyId
            
                    val destName = s"${hierarchyId}_${"%03d".format(split)}_phrases.json"
                    val dest = s"${destinationFolder}/${destName}"
                    
                    //println(s"$hierarchy  ------> $pHierarchy --------> $line")
            
                    var options = Array(StandardOpenOption.APPEND)
                    if(newFile) {
                        options = Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
                        val fileEntry = 
                            ((if(clusterChanged && !firstFile) "]\n" else "")
                            + (if(firstFile) "" else ", ")
                            + (if(clusterChanged) "\""+hierarchy.mkString(",")+"\":[" else "")
                            + "\""+destName+"\""
                            + "\n")
                        Files.write(Paths.get(fileIndex), fileEntry.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
                        firstFile = false;
                    }
                    val lineToWrite = ((if(line == 0) "[" else ", ") +
                            "{\"docId\":"+ docId +", \"phraseId\":"+phraseId +", \"hierarchyId\":\""+fullHierarchy.mkString(",") +"\""+
                            ",\"similarity\":"+(distance match {case Some(s) => "%.5f".formatLocal(java.util.Locale.US, 1-s) case _ => "null" }) +
                            ", \"before\":\""+phrases.flatMap(p => p match {case (id, text) => if(id < phraseId) Some(text) else None}).mkString("")+"\""+
                            ", \"phrase\":\""+phrases.flatMap(p => p match {case (id, text) => if(id == phraseId) Some(text) else None}).mkString("")+"\""+
                            ", \"after\":\""+phrases.flatMap(p => p match {case (id, text) => if(id > phraseId) Some(text) else None}).mkString("")+"\"}\n"
                    )
            
                    if(prevDest!="" && prevDest != dest)
                        Files.write(Paths.get(prevDest), "]".getBytes(StandardCharsets.UTF_8),StandardOpenOption.APPEND);
                    prevDest = dest
        
                    Files.write(Paths.get(dest), lineToWrite.getBytes(StandardCharsets.UTF_8), options:_*);
            }}}
            if(prevDest!="") Files.write(Paths.get(prevDest), "]".getBytes(StandardCharsets.UTF_8),StandardOpenOption.APPEND);
            Files.write(Paths.get(fileIndex), "]}".getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND);
            
        }
        if(context) {
            println("writing context output file")
            //getting tagged clusters
            val taggedHierarchy = spark.read.parquet(this.hirarchicalClusters).as[Cluster].filter(c => c.tags.size > 0).rdd.toDS
            val CollectedTaggedHierarchy  = taggedHierarchy.collect
            import spark.implicits._
            
            //Context parts to keep: tags, filters
            //Context parts to reset: expandedNodes, collapsedNodes
            //context parts to rewrite:selectedNodeggedNodes, taggedNodesReverse, taggedPhrases
            
            val context2Keep = spark.read.option("multiline", true).json(this.userContextPath.last).select($"tags", $"filters")
            val contextPlusReset = context2Keep.withColumn("expandedNodes", typedLit[Map[String,String]](Map[String,String]())).withColumn("collapsedNodes", typedLit[Map[String,String]](Map[String,String]()))
            val contextPlusSelected = contextPlusReset.withColumn("selectedNodes", typedLit[Map[String,Boolean]](CollectedTaggedHierarchy.map(c => (c.hierarchy.mkString(","), true)).toMap))
            val contectPlusTaggedNodes = contextPlusSelected.withColumn("taggedNodes", typedLit[Map[String,Seq[String]]](CollectedTaggedHierarchy.map(c => (c.hierarchy.mkString(","), c.tags)).toMap)) 
            val contectPlusTaggedNodesReverse = contectPlusTaggedNodes.withColumn("taggedNodesReverse", typedLit[Map[String,Seq[String]]](
                                                                                    {
                                                                                        val ret = scala.collection.mutable.Map[String, Seq[String]]()
                                                                                        CollectedTaggedHierarchy.map(c => (c.hierarchy.mkString(","), c.tags))
                                                                                                .foreach(p => p match {case (hierarchy, tags) => tags
                                                                                                    .foreach(t => {
                                                                                                        if(!ret.contains(t)) ret += (t -> Seq(hierarchy))
                                                                                                        else ret += (t -> (ret(t) ++ Seq(hierarchy)))
                                                                                                    })
                                                                                                })
                                                                                        ret.toMap
                                                                                    }))
                                                                                    
            
        
            val taggedPhrases = spark.read.parquet(this.phraseHierarchy).as[SemanticPhrase].rdd.toDS
            val forcedTags = TaggedPhrase.userContext2Dataset(this.userContextPath, spark)
        
            val newForced = forcedTags.joinWith(taggedPhrases, $"_1.docId" === $"_2.docId" && $"_1.phraseId" === $"_2.phraseId")
                .map(p => p match {case (forcedPhrase, phraseOnHierarchy) => (forcedPhrase, phraseOnHierarchy.hierarchy)}).toDF("forcedPhrase", "hierarchy").as[(TaggedPhrase, Seq[Int])]
                .joinWith(taggedHierarchy, $"_1.hierarchy" === $"_2.hierarchy")
                .flatMap(p => p match {case ((forcedPhrase, hierarchy), cluster) => {
                    val missingTag = forcedPhrase.isTag.toSet.diff(cluster.tags.toSet)
                                        .map(t => (s"${forcedPhrase.docId}@${forcedPhrase.phraseId}@$t",true)).toSeq
                    val wrongTags = cluster.tags.toSet.intersect(forcedPhrase.isNotTag.toSet)
                                        .map(t => (s"${forcedPhrase.docId}@${forcedPhrase.phraseId}@$t",false)).toSeq
                    missingTag ++ wrongTags
                    
                }}).collect.toMap
            
            val contextPlusTaggedPhrases = contectPlusTaggedNodesReverse.withColumn("taggedPhrases", typedLit[Map[String,Boolean]](newForced)) 
            contextPlusTaggedPhrases.write.mode("overwrite").json("/tmp/deleteme")
        
            val json = spark.sparkContext.textFile("/tmp/deleteme").first
        
            val options = Array(StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)
            Files.write(Paths.get(this.contextOutputPath), json.getBytes(StandardCharsets.UTF_8), options:_*);
        }            
    }
}

package demy.mllib.text

import demy.mllib.util.log._
import demy.mllib.linalg.implicits._
import java.sql.Timestamp
import org.apache.spark.ml.{Transformer, Estimator, PipelineStage}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vector => MLVector, SparseVector, DenseVector}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.commons.text.similarity.LevenshteinDistance
import org.apache.spark.sql.functions.{udf, col, lower, lit}

class WordCoocurrenceRefiner(override val uid: String) extends Estimator[WordCoocurrenceRefinerModel] with EntityCalculator {
    override def fit(dataset: Dataset[_]):WordCoocurrenceRefinerModel = {
        //Use case: Be added as a new entity part for entities tagged as refine. if coocurrence frequency with tagged entity is higuer than general coocurrence and not already tagged
        val spark = dataset.sparkSession
        val sc = spark.sparkContext
        import spark.implicits._
        debug("Fitting Word coocurrence refiner")
        val toRefine = sc.broadcast(this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol))===lit("refine")))
                                                .flatMap{case(tagGroup, tag, entity) => this.splitIfSingle(entity).map(e => (tagGroup, tag, e))}
                                                .collect)
        if(toRefine.value.size > 0) {
        val maxNewEntitiesVal = get(maxPropositions) match {case Some(max)=> max case _ => Int.MaxValue}
        val currentIterationValue = get(currentIteration).get
        debug("entities collected")

        val shrinkCountsAfter = 500
        val getTopNTokens = 100
        val minMatchScore = getOrDefault(minScoreMatch)

        val toRefineWithTagCounts = dataset.select(textTagGroupCol, textTagCol, textTokensCol, textVectorsCol)
                .as[(String, String, Seq[String], Seq[MLVector])]
                 .flatMap(t => t match {case (textTagGroup, textTag, textTokens, textVectors) => {
                    toRefine.value
                        .flatMap(r => r match {case (entTagGroup, entTags, entity) => {
                            if(textTagGroup != null && entTagGroup != textTagGroup )
                                None
                            else {
                                if(!this.entityInText(tokens = textTokens, vectors = textVectors, entity = entity, minScore = minMatchScore)) {
                                    None
                                } else {
                                    Some((entTagGroup, entTags, entity, 1, this.nonEntityTokens(tokens = textTokens, vectors = textVectors, entity = entity, minScore = maxNewEntitiesVal).map{case(token, vector) => (token -> (1, vector))}.toMap))
                                }
                            }
                        }})
                }})
                .groupByKey(t => t match {case (entTagGroup, entTags, entity, entityCount, otherTokensVectorsCount)=> (entTagGroup, entTags, entity)})
                .reduceGroups((t1, t2) => (t1, t2) match {case ((entTagGroup1, entTags1, entity1, entityCount1, otherTokensVectorsCount1),(entTagGroup2, entTags2, entity2, entityCount2, otherTokensVectorsCount2)) => {
                    (entTagGroup1, entTags1, entity1, entityCount1 + entityCount2, (otherTokensVectorsCount1 ++ otherTokensVectorsCount2)
                                                                                            .map{ case (token,(count, vector)) => 
                                                                                                        (token -> ((otherTokensVectorsCount1.get(token), otherTokensVectorsCount2.get(token)) match {
                                                                                                            case (Some(p1), None) => p1
                                                                                                            case (None, Some(p2)) => p2
                                                                                                            case (Some((count1, vector1)), Some((count2, vector2))) => (count1 + count2, vector1)
                                                                                                            case _ => throw new Exception("This case should never happend @epi")
                                                                                                        })) 
                                                                                                
                                                                                            }.toSeq.sortWith(_._1 > _._1).slice(0,shrinkCountsAfter).toMap) 
                }})
                .map{case (_,(entTagGroup, entTags, entity, entityCount, otherTokensVectorsCount)) => (entTagGroup, entTags, entity, entityCount, otherTokensVectorsCount.slice(0, getTopNTokens).toMap)}
                .collect
        toRefine.destroy

        debug(s"getting tokens to count ${toRefineWithTagCounts.size}")
        val otherTokensToCount = sc.broadcast(toRefineWithTagCounts.map{case (entTagGroup, entTags, entity, entityCount, otherTokensVectorsCount) => (entTagGroup, otherTokensVectorsCount.keys.toSet)}
                                                                    .groupBy{case(entTaGroup, tokens) => entTaGroup}
                                                                    .map{case (entTaGroup, tokens) => (entTaGroup, tokens.map(_._2).reduce(_ ++ _))}.toMap)
        debug("tokens to couns defined")

        debug("counting tokens")
        val (tagGroupTokensCounts, tagGroupCounts) 
             = dataset.select(textTagGroupCol, textTagCol, textTokensCol)
                .as[(String, String, Seq[String])]
                .flatMap{case (textTagGroup, textTag, textTokens) => otherTokensToCount.value.get(textTagGroup) match {case(Some(otherTokens))=> Some((otherTokens.intersect(textTokens.toSet).map(t => ((textTagGroup, t) -> 1)).toMap, Map(textTagGroup -> 1))) case _ => None}}
                .reduce((p1, p2) => (p1, p2) match { case ((tokenCounts1, groupCount1), (tokenCounts2, groupCount2)) => 
                    (
                        (tokenCounts1 ++ tokenCounts2).map{ case (k,v) => k -> (tokenCounts1.getOrElse(k,0) + tokenCounts2.getOrElse(k,0)) }
                        ,(groupCount1 ++ groupCount2).map{ case (k,v) => k -> (groupCount1.getOrElse(k,0) + groupCount2.getOrElse(k,0)) }
                    )
                })
        debug("tokens counted")
        otherTokensToCount.destroy
        
        val refinedEntities = toRefineWithTagCounts.map{ case (entTagGroup, entTags, entity, entityCount, otherTokensVectorsCount) => 
            val bestTokens = otherTokensVectorsCount.keys.toSeq.map{token => (token -> {
                val entityFreq = entityCount.toDouble
                val totalFreq = tagGroupCounts(entTagGroup).toDouble
                val tokenFreq = tagGroupTokensCounts.getOrElse((entTagGroup, token), 0).toDouble
                val cooFreq = otherTokensVectorsCount(token)._1.toDouble
                val vector = otherTokensVectorsCount(token)._2
                val tokenInRatio  = cooFreq / entityFreq
                val tokenGlobalRatio = tokenFreq/totalFreq
                val coocurrenceIndex = tokenInRatio / tokenGlobalRatio
                val score = cooFreq * coocurrenceIndex
                (score, vector)
                })}
                .sortWith(_._2._1 > _._2._1)
                .slice(0,maxNewEntitiesVal)
            if(bestTokens.size == 0) 
                (entTagGroup, entTags, entity)
            else 
                (entTagGroup, entTags, entity.map(legs => 
                                    legs.map{case(token, vector, trace) => (token, vector, trace.setUserStatus("refined", get(currentIteration).get))}) ++ 
                                        Seq(bestTokens.map{case (token, (score, vector)) => (Seq(token), vector, this.proposedTrace(iteration = currentIterationValue, score= score ))})
                )
                
        }

        val existingEntities = this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol)).isin(lit("ok"), lit("proposed"))))
                                                .collect

        new WordCoocurrenceRefinerModel(uid, refinedEntities ++ existingEntities).seTextsColNames(get(textsColNames).get).setEntitiesColNames(get(entitiesColNames).get).setEntitiesDF(get(entitiesDF).get).setCurrentIteration(get(currentIteration).get).setParent(this)
                .setOutEntitiesDF()
        }
        else {
          val existingEntities = this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol)).isin(lit("ok"), lit("proposed"))))
                                                .collect
          new WordCoocurrenceRefinerModel(uid, existingEntities).seTextsColNames(get(textsColNames).get).setEntitiesColNames(get(entitiesColNames).get).setEntitiesDF(get(entitiesDF).get).setCurrentIteration(get(currentIteration).get).setParent(this)
                .setOutEntitiesDF()
        }
        
    }
    def this() = this(Identifiable.randomUID("WordCoocurrenceRefiner"))
}

class WordCoocurrenceRefinerModel(override val uid: String, override val outEntities:Array[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])]) extends org.apache.spark.ml.Model[WordCoocurrenceRefinerModel] with EntityCalculatorModel {
    def this(outEntities:Array[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])]) = this(Identifiable.randomUID("WordCoocurrenceRefinerModel"), outEntities)
}

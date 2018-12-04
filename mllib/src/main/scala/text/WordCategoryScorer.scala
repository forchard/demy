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

class WordCategoryScorer(override val uid: String) extends  Estimator[WordCategoryScorerModel] with EntityCalculator {
    override def fit(dataset: Dataset[_]): WordCategoryScorerModel = {
        val spark = dataset.sparkSession
        val sc = spark.sparkContext
        import spark.implicits._
        debug("fitting WordCategoryScorer")
        val entitiesToIgnore = sc.broadcast(this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol)).isin(("ignore"), lit("ok"), lit("proposed"), lit("refine"), lit("refined")))).collect)
        val entitiesToIgnoreOnTag = sc.broadcast(this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol))===lit("other"))).collect)

        debug("Entities collected")
        val tokensByTag = dataset.select(textTagGroupCol, textTagCol,  textTokensCol, textVectorsCol).as[(String, String, Seq[String], Seq[MLVector])]
                        .map{case(textGroup, textTag, textTokens, textVectors) => (textGroup, textTag, {
                                textTokens.zip(textVectors).zipWithIndex.filter{case((textToken, textVector), i) => 
                                    (!entitiesToIgnore.value.exists{case (entGroup, entTag, legs) => entGroup == textGroup && legs.exists{ leg => leg.exists { 
                                       case(synTokens, synVector, synTrace) =>  synTokens.size > 0 && synTokens == textTokens.slice(i, i + synTokens.size)
                                    }}}
                                    && !entitiesToIgnoreOnTag.value.exists{case (entGroup, entTag, legs) => entGroup == textGroup && entTag == textTag && legs.exists{ leg => leg.exists { 
                                       case(synTokens, synVector, synTrace) =>  synTokens.size > 0 && synTokens == textTokens.slice(i, i + synTokens.size)
                                    }}})
                                }.map{case ((textToken, textVector), i) => (textToken, textVector)}
                            })
                        }
                        .map{case(textGroup, textTag, textTokensVector) => (textGroup, textTag, Set(textTokensVector.map{case(textTokens, textVectors)=>textTokens}), textTokensVector.toMap)}
                        .groupByKey{case(textGroup, textTag, texts, textVectors) => (textGroup, textTag)}
                        .reduceGroups((t1, t2) => (t1, t2) match {case ((textGroup1, textTag1, texts1, textVectors1),(textGroup2, textTag2, texts2, textVectors2)) =>  (textGroup1, textTag1, texts1 ++ texts2, textVectors1 ++ textVectors2)}).map(p => p._2)
                        .map{case(textGroup, textTag, texts, textVectors) => 
                            (textGroup, textTag, textVectors, texts.toSeq.flatMap{case(textTokens) => textTokens.map(t => (t, 1))}.groupBy{case(token, one) => token}.map{case(token, ones) => (token, ones.size)})}
                        .toDF(entTagGroupCol, entTagCol, "tokenVectorsMap", "tokenCountsMap")
                        .as[(String, String, Map[String, DenseVector], Map[String, Int])]
                        .cache

        val idf = sc.broadcast({
            //log_e(Total number of documents / Number of documents with term t in it)
            val collected = tokensByTag.collect
            collected.map{case(entGroup, entTag, tokenVectorsMap, tokenCountsMap) => (entGroup, tokenCountsMap.map{case(token, count) => (token, 1)})}
                                     .groupBy{case(entGroup, tokenCountsMap) => entGroup}
                                     .map{case(entGroup, countsByTagList) => (entGroup, (countsByTagList.size, countsByTagList.map{case(_, tokenMap)=>tokenMap}.reduce((map1, map2)=> (map1 ++ map2).keys.map(k => (k, map1.getOrElse(k, 0) + map2.getOrElse(k, 0))).toMap)))}
                                     .map{case(entGroup, (tagCount, countsOnDocuments)) => (entGroup, countsOnDocuments.map{case (token, docCount) => (token, Math.log(tagCount.toDouble / docCount ))})}
        })
        
        debug("idf calculated")

                        
        val maxNewEntitiesVal = get(maxPropositions) match {case Some(max)=> max case _ => Int.MaxValue}
        val currentIterationValue = get(currentIteration).get

        val tokensByTagWithScores = tokensByTag
                            .select( entTagGroupCol, entTagCol, "tokenVectorsMap" , "tokenCountsMap").as[(String, String, Map[String, DenseVector], Map[String, Int])]
                            .map{case (entGroup, entTag, tokenVectorsMap, tokenCountsMap) => 
                                        (entGroup, entTag
                                            , tokenCountsMap.toSeq.map{ case (entToken, tf) => (entToken, tf * idf.value(entGroup)(entToken))}
                                                                  .sortWith(_._2 > _._2)
                                                                  .take(maxNewEntitiesVal * 10)
                                                                  .map{ case (entToken, tfidf) => (entToken, tfidf, tokenVectorsMap(entToken))}
                                        )}
                            .map{case (entGroup, entTag, topTokenScoresVectors) 
                                => (entGroup, entTag
                                    , topTokenScoresVectors.map{case(entToken, tfidf, entVector)=>entToken}
                                    , {topTokenScoresVectors.map{case(entToken, tfidf, entVector)=>tfidf} 
                                            match { case(tfidfs) =>
                                                val sum = tfidfs.sum
                                                tfidfs.map(tfidf => tfidf/sum)
                                        }}
                                    , topTokenScoresVectors.map{case(entToken, tfidf, entVector)=>entVector}
                                    )
                                }
                            .toDF(entTagGroupCol, entTagCol, "entTokens", "entScores", "entVectors")
                            .cache
        
        debug("getting candidates")
        
        val c1 = tokensByTagWithScores.as[(String, String, Seq[String], Seq[Double], Seq[MLVector])]
        val c2 = tokensByTagWithScores.as[(String, String, Seq[String], Seq[Double], Seq[MLVector])]
        //candidates are are associated to a tag if they have the highest score tf-idf (on %) * cosine similarity against against all tag sentences   
        val entitiesScored = c1.joinWith(c2, c1(entTagGroupCol)===c2(entTagGroupCol), "cross")
           .filter(p => p match {case ((tagGroup1, tag1, tokens1, assciatedScore1, docVector1), (tagGroup2, tag2, tokens2, assciatedScore2, docVector2))
              => (tag1!=null || tag2!=null) && tag1!= tag2})
           .map(p => p match {case ((tagGroup1, tag1, tokens1, assciatedScore1, docVector1), (tagGroup2, tag2, tokens2, assciatedScore2, docVector2))
              => {
                  val sumVect1 = docVector1.filter(v => v != null) match{case v =>  if(v.size > 0) v.reduce((v1, v2)=> v1.sum(v2)) else null}
                  val sumVect2 = docVector2.filter(v => v != null) match{case v =>  if(v.size > 0) v.reduce((v1, v2)=> v1.sum(v2)) else null}
                  val t1 = tokens1.zip(assciatedScore1.zip(docVector1))
                  val t2 = tokens2.zip(assciatedScore2.zip(docVector2)).toMap
                  val inTag = t1.filter(t => t match {case (token1, (score1, vector1)) =>{
                      t2.get(token1) match {
                          case Some((score2, vector2)) if vector1 !=null && vector2 != null && score2 * vector2.cosineSimilarity(sumVect2.minus(vector2)) >= score1 * vector1.cosineSimilarity(sumVect1.minus(vector1)) => false
                          case Some((score2, vector2)) if (vector1 ==null || vector2 == null) && score2 >= score1 => false
                          case _ => true
                      
                  }}})
                  
                  (tagGroup1, tag1
                      , inTag.map{case (token1, (score1, vector1))=>(token1, (if(vector1 != null)  score1 * vector1.cosineSimilarity(sumVect1.minus(vector1)) else score1, vector1))}
                      )
                  
              }})//until here each tag Group has removed tokens having better association on another tag
           .groupByKey(t => t match {case (tagGroup, tag, tokenScoreVectors)=> (tagGroup, tag)})
           .reduceGroups((t1, t2) => (t1, t2) match {case ((tagGroup1, tag1, tokenScoreVectors1), (tagGroup2, tag2, tokenScoreVectors2))
              => {
                    val t2tokens = tokenScoreVectors2.map{case(token, (score, vector))=>token}.toSet
                    val inTag = tokenScoreVectors1.filter{case (token1, (score1, vector1)) => t2tokens.contains(token1)}.sortWith(_._2._1 > _._2._1).take(maxNewEntitiesVal)
                  
                    (tagGroup1, tag1, inTag)
              }})
            .map{case (_, (tagGroup, tag, tokenScoreVectors)) => (tagGroup, tag, Seq(tokenScoreVectors.map{case (token, (score, vector)) => (Seq(token), vector, this.proposedTrace(iteration = currentIterationValue, score = score))}))}
            .collect
        debug("candidates collected")
        entitiesToIgnore.destroy
        entitiesToIgnoreOnTag.destroy

        val existingEntities = this.groupEntities(get(entitiesDF).get.where(lower(col(entUserStatusCol)).isin(lit("ok"), lit("proposed"), lit("refine"), lit("refined")))).collect      
        new WordCategoryScorerModel(uid, entitiesScored ++ existingEntities).seTextsColNames(get(textsColNames).get).setEntitiesColNames(get(entitiesColNames).get).setEntitiesDF(get(entitiesDF).get).setCurrentIteration(get(currentIteration).get).setParent(this)
                .setOutEntitiesDF()            
    }
    def this() = this(Identifiable.randomUID("WordCategoryScorer"))
}

class WordCategoryScorerModel(override val uid: String, override val outEntities:Array[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])]) extends org.apache.spark.ml.Model[WordCategoryScorerModel] with EntityCalculatorModel {
    def this(outEntities:Array[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])]) = this(Identifiable.randomUID("WordCoocurrenceRefinerModel"), outEntities)
}

package demy.mllib.text

import demy.mllib.util.log._
import demy.mllib.linalg.implicits._
import java.sql.Timestamp
import org.apache.spark.ml.{Transformer, Estimator, PipelineStage}
import org.apache.spark.ml.param.{Param, Params, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vector => MLVector, SparseVector, DenseVector}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.commons.text.similarity.LevenshteinDistance
import org.apache.spark.sql.functions.{udf, col, lower, lit, row_number}
import org.apache.spark.sql.expressions.Window

trait EntityCalculator extends PipelineStage  {
    final val textsColNames = new Param[Map[String, String]](this, "textsColNames", "The texts column name mappings defaults are: tagGroup, tag, tokens")
    final val entitiesColNames = new Param[Map[String, String]](this, "entitiesColNames", "The entities column name mappings defaults are: tagGroup, tag, entityGroup, entityIndex, synonym, iteration, userStatus, changed, score, changedOn, stability")
    final val entitiesDF = new Param[DataFrame](this, "entitiesDF", "The current entities")
    final val currentIteration = new Param[Int](this, "currentIteration", "The current iteration to be traced")
    final val maxPropositions = new Param[Int](this, "maxPropositions", "The maximum numbers of new words to be associated a category")
    final val minScoreMatch = new Param[Double](this, "minScoreMatch", "The minimum score of two words to ve considered as synonyms")

    def seTextsColNames(value: Map[String, String]): this.type = set(textsColNames, value)
    def setEntitiesColNames(value: Map[String, String]): this.type = set(entitiesColNames, value)
    def setEntitiesDF(value: DataFrame): this.type = set(entitiesDF, value)
    def setCurrentIteration(value: Int): this.type = set(currentIteration, value)
    def setMaxPropositions(value: Int): this.type = set(maxPropositions, value)
    def setMinScoreMatch(value: Double): this.type = set(minScoreMatch, value)
    
    override def transformSchema(schema: StructType): StructType = schema
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}


    setDefault(textsColNames -> Map("id"->"id", "tagGroup"->"tagGroup", "tag"->"tag", "tokens"->"tokens", "userStatus"->"userStatus", "vectors"->"vectors", "entities"->"entities", "text"->"text"
                                            , "iteration"->"iteration", "userStatus"->"userStatus", "changed"->"changed", "score"->"score", "changedOn"->"ChangedOn", "stability"->"stability")
                , entitiesColNames -> Map("tagGroup"->"tagGroup", "tag"->"tag", "entityGroup"->"entityGroup", "entityIndex"->"entityIndex", "synonym"->"synonym", "synonymVectors"->"synonymVectors"
                                            , "iteration"->"iteration", "userStatus"->"userStatus", "changed"->"changed", "score"->"score", "changedOn"->"ChangedOn", "stability"->"stability")
                , minScoreMatch -> 0.8
                )
    def entTagGroupCol = getOrDefault(entitiesColNames)("tagGroup")
    def entTagCol = getOrDefault(entitiesColNames)("tag")
    def entEntityGroupCol = getOrDefault(entitiesColNames)("entityGroup")
    def entEntityIndexCol = getOrDefault(entitiesColNames)("entityIndex")
    def entSynonymCol = getOrDefault(entitiesColNames)("synonym")
    def entSynonymVectorCol = getOrDefault(entitiesColNames)("synonymVectors")
    def entUserStatusCol = getOrDefault(entitiesColNames)("userStatus")
    def traceEntCols = Array(getOrDefault(entitiesColNames)("iteration"), getOrDefault(entitiesColNames)("userStatus"), getOrDefault(entitiesColNames)("changed")
                        , getOrDefault(entitiesColNames)("score"), getOrDefault(entitiesColNames)("changedOn"), getOrDefault(entitiesColNames)("stability"))

    def textIdCol = getOrDefault(textsColNames)("id")
    def textTextCol = getOrDefault(textsColNames)("text")
    def textTagGroupCol = getOrDefault(textsColNames)("tagGroup")
    def textTagCol = getOrDefault(textsColNames)("tag")
    def textEntitiesCol = getOrDefault(textsColNames)("entities")
    def textTokensCol = getOrDefault(textsColNames)("tokens")
    def textVectorsCol = getOrDefault(textsColNames)("vectors")
//    def textUserStatusCol = getOrDefault(textsColNames)("userStatus")
    def traceTextCols = Array(getOrDefault(textsColNames)("iteration"), getOrDefault(textsColNames)("userStatus"), getOrDefault(textsColNames)("changed")
                            , getOrDefault(textsColNames)("score"), getOrDefault(textsColNames)("changedOn"), getOrDefault(textsColNames)("stability"))
    def setTextColName(mappings:(String, String)*) : this.type = set(textsColNames, getOrDefault(textsColNames) ++ mappings.toMap)
    def setEntColName(mappings:(String, String)*) : this.type = set(entitiesColNames, getOrDefault(entitiesColNames) ++ mappings.toMap)

    def proposedTrace(iteration:Int, score:Double) = Trace(iteration=iteration, userStatus = "proposed", score = score)

    def entityDS(ds:Dataset[_]):Dataset[(String, String, Int, Int, Seq[String], MLVector, Trace)] = {
            import ds.sparkSession.implicits._
            ds.select(col(entTagGroupCol), col(textTagCol), col(entEntityGroupCol) , col(entEntityIndexCol), col(entSynonymCol), col(textVectorsCol), Trace.traceExp(getOrDefault(entitiesColNames)))
                .as[(String, String, Int, Int, Seq[String], MLVector, Trace)]
    }
    def textDS(ds:DataFrame):Dataset[(String, String, String, Seq[String], Trace, Seq[String], Seq[MLVector], String)] = {
            import ds.sparkSession.implicits._
            ds.select(col(textIdCol), col(textTagGroupCol), col(textTagCol), col(textEntitiesCol), Trace.traceExp(), col(textTokensCol), col(textVectorsCol),col(textTextCol))
                .as[(String, String, String, Seq[String], Trace, Seq[String], Seq[MLVector], String)]
    }
    def textDF(ds:Dataset[(String, String, String, Seq[String], Trace, Seq[String], Seq[MLVector], String)]):DataFrame = {
            import ds.sparkSession.implicits._
            ds.map{case(id, tagGroup, tag, entities, trace, tokens, vectors, text) => (id, tagGroup, tag, entities, tokens, vectors, text, trace.iteration, trace.userStatus, trace.changed, trace.score, trace.changedOn, trace.stability)}
              .toDF((Array(textIdCol, textTagGroupCol, textTagCol, textEntitiesCol, textTokensCol, textVectorsCol, textTextCol) ++ traceTextCols ):_*)
    }
    def groupEntities(ds:Dataset[_]):Dataset[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])] = {
            import ds.sparkSession.implicits._
            val allds = entityDS(ds)
               .map(t => t match {case (group, category, entityGroup, entityIndex, synonym, vector, trace) => (group, category, entityGroup, entityIndex, Seq((synonym, vector, trace)))})
               .groupByKey(t => t match {case (group, category, entityGroup, entityIndex, synonyms) => (group, category, entityGroup, entityIndex)})
               .reduceGroups((t1, t2) => (t1, t2) match {case ((group1, category1, entityGroup1, entitiIndex1, synonyms1),(group2, category2, entityGroup2, entitiIndex2, synonyms2)) => (group1, category1, entityGroup1, entitiIndex1, synonyms1 ++ synonyms2)})
               .map(t => t._2 match {case (group, category, entityGroup, entityIndex, synonyms) => (group, category, entityGroup, Map(entityIndex -> synonyms))})
               .groupByKey(t => t match {case (group, category, entityGroup, synonyms) => (group, category, entityGroup)})
               .reduceGroups((t1, t2) => (t1, t2) match {case ((group1, category1, entityGroup1, synonyms1),(group2, category2, entityGroup2, synonyms2)) => (group1, category1, entityGroup1, synonyms1 ++ synonyms2)})
               .map(t => t._2 match {case (group, category, entityGroup, synonyms) => (group, category, synonyms.values.toSeq)})
               
            val singleSizes = allds.filter(t => t match {case (group, category, synonyms) => synonyms.size == 1})
                                .groupByKey{case (group, category, synonyms) => (group, category)}
                                .reduceGroups((g1, g2)=>(g1, g2) match {case((group, cat, entity1),(_, _, entity2)) => (group, cat, Seq(entity1(0) ++ entity2(0)))})
                                .map(_._2)
                                
            val biggerSizes = allds.filter(t => t match {case (group, category, synonyms) => synonyms.size> 1})
            singleSizes.union(biggerSizes)

    }
    def singleEntities(ds:Dataset[_], mapping:Map[String, String]=Map[String, String]()):Dataset[(String, String, Seq[(Seq[String], MLVector, Trace)])] = {
        import ds.sparkSession.implicits._
        groupEntities(ds).flatMap{case(group, category, legs) => if(legs.size ==1) Some(group, category, legs(0)) else None}
    }
    def ungroupEntities(entities:Array[(String, String, Seq[Seq[(Seq[String], MLVector,  Trace)]])]):Seq[(String, String, Int, Int, Seq[String], MLVector, Trace)] = {
        entities.groupBy{case(group, category, groupedSyns)=> (group, category)}.values.map(grouped => grouped.zipWithIndex.map{case((group, category, groupedSyns),i) => (group, category, i, groupedSyns)}).flatMap(a => a).toSeq
                .flatMap{case(group, category, entityGroup, legs)=> legs.zipWithIndex.map{case(leg, index) => (group, category, entityGroup, index, leg)}}
                .flatMap{case(group, category, entityGroup, entityIndex, leg)=> leg.map{case(tokens, vector, trace) => (group, category, entityGroup, entityIndex, tokens, vector, trace)}}
    }
    def ungroupEntitiesAsDF(entities:Array[(String, String, Seq[Seq[(Seq[String], MLVector,  Trace)]])]):DataFrame = {
        val spark = get(entitiesDF).get.sparkSession
        import spark.implicits._
        ungroupEntities(entities)
            .toDS
            .map{case(group, category, entityGroup, entityIndex, tokens, vector, trace) => (group, category, entityGroup, entityIndex, tokens, vector, trace.iteration, trace.userStatus, trace.changed, trace.score, trace.changedOn, trace.stability)}
            .toDF((Array(entTagGroupCol, entTagCol, entEntityGroupCol, entEntityIndexCol, entSynonymCol, entSynonymVectorCol) ++ traceEntCols) :_*)    
    }
    def ungroupEntities(entities:Dataset[(String, String, Seq[Seq[(Seq[String], MLVector,  Trace)]])]):DataFrame = {
        import entities.sparkSession.implicits._
        entities.toDF("tagGroup", "tag", "entity")
            .withColumn("groupId", row_number().over(Window.partitionBy($"tagGroup", $"tag"))).as[(String, String, Seq[Seq[(Seq[String], MLVector,  Trace)]], Int)]
            .flatMap{case(group, category, legs, entityGroup)=> legs.zipWithIndex.map{case(leg, index) => (group, category, entityGroup, index, leg)}}
            .flatMap{case(group, category, entityGroup, entityIndex, leg)=> leg.map{case(tokens, vector, trace) => (group, category, entityGroup, entityIndex, tokens, vector, trace)}}
            .map{case(group, category, entityGroup, entityIndex, tokens, vector, trace) => (group, category, entityGroup, entityIndex, tokens, vector, trace.iteration, trace.userStatus, trace.changed, trace.score, trace.changedOn, trace.stability)}
            .toDF((Array(entTagGroupCol, entTagCol, entEntityGroupCol, entEntityIndexCol, entSynonymCol, entSynonymVectorCol) ++ traceEntCols) :_*)    
    }
    def entityInText(tokens:Seq[String], vectors:Seq[MLVector], entity:Seq[Seq[(Seq[String], MLVector, Trace)]], minScore:Double) = {
            entity.forall(entityLeg => entityLeg.exists{case(synExp, synVector, trace) => 
                Range(0, tokens.size).exists{i => //137
                    (i + synExp.size <= tokens.size
                    && (Range(0, synExp.size).forall(j => this.matchingScore(synExp(j), tokens(i+j)) >= minScore)
                        || (synVector != null && Range(0, synExp.size).forall(j => vectors(i+j) != null) && Range(0, synExp.size).map(j => vectors(i + j)).reduce((v1, v2) => v1.sum(v2)).cosineSimilarity(synVector) >= minScore)
                        )
                    )
                }})
    }
    def nonEntityTokens(tokens:Seq[String], vectors:Seq[MLVector], entity:Seq[Seq[(Seq[String], MLVector, Trace)]], minScore:Double) = {
            tokens.zip(vectors).zipWithIndex.filter{case((token, vector), i) => 
                !entity.forall(entityLeg => entityLeg.exists{case(synExp, synVector, trace) => 
                    (i + synExp.size <= tokens.size
                    && (Range(0, synExp.size).forall(j => this.matchingScore(synExp(j), tokens(i+j)) >= minScore)
                        || (synVector != null && Range(0, synExp.size).forall(j => vectors(i+j) != null) && Range(0, synExp.size).map(j => vectors(i + j)).reduce((v1, v2) => v1.sum(v2)).cosineSimilarity(synVector) >= minScore)
                        )
                    )
                })}
                .map{case((token, vector), i) => (token, vector)}
    }
    def printEntity(e:(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])) = e match {case(tagGroup, tag, entity) =>  println(s"($tagGroup)->($tag)");entity.foreach(leg => {println(s"Leg>> ${leg.map{case(tokens, vector, trace) => tokens.mkString("+")}.mkString(",")}")})}
    def splitIfSingle(entity:Seq[Seq[(Seq[String], MLVector, Trace)]]):Seq[Seq[Seq[(Seq[String], MLVector, Trace)]]] = if(entity.size > 1) Seq(entity) else entity(0).map(syn => Seq(Seq(syn)))
    def matchingScore(token1:String, token2:String, vector1:MLVector=null, vector2:MLVector=null) = {
        val levenshtein = new LevenshteinDistance(1)
        if(token1.toLowerCase == token2.toLowerCase) 1.0 
        else if(token1.size > 3 && levenshtein(token1.toLowerCase, token2.toLowerCase)>0) 1.0 - levenshtein(token1.toLowerCase, token2.toLowerCase).toDouble/token1.size
        else if(vector1 !=null && vector2!=null) vector1.cosineSimilarity(vector2)
        else 0.0
    }
};trait EntityCalculatorModel extends EntityCalculator {
    val outEntities:Array[(String, String, Seq[Seq[(Seq[String], MLVector, Trace)]])]
    final val outEntitiesDF = new Param[DataFrame](this, "outEntitiesDF", "The current trandformed entities after fitting the model")
    def setOutEntitiesDF(): this.type = set(outEntitiesDF, this.ungroupEntitiesAsDF(outEntities))
    def transform(dataset: Dataset[_]):DataFrame = {
        val spark = dataset.sparkSession
        val sc = spark.sparkContext
        import spark.implicits._

        val currentEntities = sc.broadcast(this.groupEntities(get(entitiesDF).get.where(!lower(col(entUserStatusCol)).isin("ignore", "other"))).collect)
        val currentIterationValue = get(currentIteration).get
        val minMatchScore = getOrDefault(minScoreMatch)

                        
        debug("entities collected")
        val EntitiesAssigned = this.textDS(dataset.toDF).map{ case (id, tagGroup, currentTag, entities, trace, wordsTokens, wordsVectors, text) => {
            if(trace.userStatus == "fixed")
                (id, tagGroup, currentTag, entities,trace ,wordsTokens, wordsVectors, text)
            else {
                var bestTagScore:Option[Double]=None
                var bestTag:Option[String]=None
                var bestSynonyms:Option[Seq[String]]=None
                var bestStatus:Option[String]=None
                currentEntities.value.foreach{case (dicoGroup, dicoTag, dicoEnt) => 
                     if(tagGroup == dicoGroup && (currentTag == null || currentTag == dicoTag) && dicoEnt != null && dicoEnt.size >0) {
                       //Evaluating possible tag represented by all its entities against a line represented by its tokens and vectors and we will return the best tags if any
                       val tagSize = dicoEnt.size
                       val bestSynGroup = Array.fill(tagSize)(None.asInstanceOf[Option[(String,Double, String)]])
                       dicoEnt.zipWithIndex.foreach{case (leg, partIndex) => {
                           //Evaluating an entity part 
                           leg.foreach{case (synonymTokens, synVector, entTrace) => 
                           if(synonymTokens.size > 0) {
                               //Evaluating a synonym (defined by its tokens and vector) against a text (defined by its tokens and vectors)
                               val synonym = synonymTokens.mkString(" ")
                               var iWord = 0
                               wordsTokens.foreach(wordToken => {
                                   //Evaluating a synonym (defined by its tokens and vector) against a word (defined by its tokens and vectors)
                                   val word = wordsTokens.slice(iWord, iWord + synonymTokens.size).mkString(" ")
                                   val tokenVector = wordsVectors(iWord)
                                   val synScore = this.matchingScore(word, synonym, tokenVector, synVector)
                                   val synStatus = entTrace.userStatus
                                   bestSynGroup(partIndex) = bestSynGroup(partIndex) match { 
                                       case Some((bestSynonym, bestScore, bestStatus)) if synScore > bestScore => Some((synonym, synScore, synStatus))
                                       case _ if synScore >= minMatchScore => Some((synonym, synScore, synStatus))
                                       case _ => bestSynGroup(partIndex)
                                   }
                                   iWord = iWord + 1
                               })
                           }}
                       }}
                       val foundSynonyms = bestSynGroup.flatMap(t => t)
                       if(foundSynonyms.size == tagSize) {
                           val tagScore = foundSynonyms.map(t => t match {case (synonym, synScore, synStatus) => synScore}).map(score => score / foundSynonyms.size).sum
                           val matchedSynonyms = foundSynonyms.map(t => t match {case (synonym, synScore, synStatus) => synonym})
                           val matchedStatus = foundSynonyms.map(t => t match {case (synonym, synScore, synStatus) => synStatus})
                               .reduce((s1, s2)=> 
                                   if(s1 == "proposed" || s2 == "proposed") "proposed" 
                                   else if(s1 == "refined" || s2 == "refined") "refined" 
                                   else if(s1 == "ok" || s2 == "ok") "ok"
                                   else throw new Exception(s"unexpected trace status betwen ($s1, $s2) @epi")
                                )
                           val thisIsBestTag = bestTagScore match {
                               case Some(bestScore) if tagScore > bestScore => true
                               case None => true
                               case _ => false
                           }
                           if(thisIsBestTag) {
                               bestTag = Some(dicoTag)
                               bestSynonyms = Some(matchedSynonyms)
                               bestTagScore = Some(tagScore) 
                               bestStatus = Some(matchedStatus)
                           }
                       }
                     }
                }
                
                (bestTag, bestSynonyms, bestTagScore, bestStatus) match { 
                    case (Some(chosenTag), Some(chosenSynonyms), Some(chosenScore), Some(chosenStatus) ) 
                        => (id, tagGroup, chosenTag, chosenSynonyms, trace.setScore(chosenScore).setChanged(chosenTag != currentTag, currentIterationValue)
                                .setUserStatus(
                                    if(chosenTag!=null && chosenStatus == "ok") "validated" 
                                    else if (chosenTag!=null) "matched" 
                                    else "proposed"
                                    , currentIterationValue)
                                , wordsTokens, wordsVectors, text)
                    case _ 
                        => (id, tagGroup, null, entities,trace.setUserStatus("pending", currentIterationValue).setChanged(null != currentTag, currentIterationValue) ,wordsTokens, wordsVectors, text)
                }
            }
          }}
        this.textDF(EntitiesAssigned)
    }
}

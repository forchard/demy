package demy.mllib.text

import demy.mllib.linalg.SemanticVector
import demy.mllib.linalg.Coordinate
import demy.mllib.util.util
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}


case class PhraseTagging(textSource:org.apache.spark.sql.DataFrame, idColumnName:Option[String]=None, commentColumnName:String, lexiquePath:String/*, semanticVectorsPath: String, taggedWordsPath:String, semanticPhrasesPath: String*/, spark:org.apache.spark.sql.SparkSession) {
    def tagWords() = {
        import spark.sqlContext.implicits._
        val lexiqueSimplified = spark.read.parquet(this.lexiquePath).as[(String, Seq[(String, String, SemanticVector, SemanticVector, SemanticVector)])]

        val source = textSource.withColumnRenamed(this.commentColumnName, "comment")
            .select($"comment".as("text"), (this.idColumnName match {
                                                               case Some(docId) => textSource(docId)
                                                               case None => row_number().over(Window.orderBy($"comment".desc))
                                                             }).as("docId")).as[Doc]
            .flatMap(doc => Word.splitDoc(doc))
            .map(w => (w.word, w.simplified, w.isWord, w.index, w.phraseId, w.docId)).toDF("word","simplified","isWord","index","phraseId","docId").as[(String,String, Boolean, Int, Int, Int )]
         source
            .joinWith(lexiqueSimplified, source("simplified")===lexiqueSimplified("simplified"), "left")
            .map(p => p match {case ((word,simplified,isWord,index,phraseId,docId),lexiqueEntry)=>(docId, Seq((word, index,phraseId,simplified,isWord
                                                                                                                    , lexiqueEntry match {case (simplified2, variants) => variants
                                                                                                                                        case _ => Seq[(String, String, SemanticVector, SemanticVector, SemanticVector)]()
                                                                                                                        })))})
            .groupByKey( t => t match {case (docId, words)=> docId})
            .reduceGroups((t1, t2) => (t1, t2) match {case ((docId1, words1),(docId2, words2)) 
                                                        => (docId1
                                                            , (words1 ++ words2).sortWith((w1, w2)=>(w1, w2) 
                                                                    match {case ((word1, index1,phraseId1,simplified1,isWord1, variants1),(word2, index2,phraseId2,simplified2,isWord2, variants2))
                                                                            => phraseId1 < phraseId2 || (phraseId1 == phraseId2 && index1 < index2) 
                                                                    }))}).map(p => p._2)//Here we have a document per row with an array of words and a set of variants associted from the soimplified lexique 
            .map(t => t match {case (docId, words) => (docId, {
                val defTags = SemanticVector(word = null.asInstanceOf[String], coord = Vector(Coordinate(index = GramTag.Nom.id, value = 1.0)))
                var prevTags:Option[SemanticVector] = None
                var i = 0

                words.zipWithIndex.map(t => t match {case ((word, index,phraseId,simplified,isWord, variants), i) => {
                    //tryToFindPrevWord
                    if(!isWord) {
                        (word, null.asInstanceOf[String], index,phraseId,simplified,isWord, None)
                    }
                    else if(variants.filter(v => v match {case (flexion, lemme, tags, afterTag, beforeTag) => tags.coord.size > 0}).size == 0){
                        prevTags = Some(defTags)
                        (word, word, index,phraseId,simplified,isWord, Some(defTags))
                    } else {
                        var nextTags:Option[Seq[SemanticVector]] = None
                        var j = i + 1
                        while(nextTags.isEmpty && j < words.size) {
                            words(j) match {case (pword, pindex, pphraseId,psimplified,pisWord, pvariants) 
                                => if(pisWord) nextTags = Some(pvariants.map(v => v match {case (flexion, lemme, tags, afterTag, beforeTag) => tags})) 
                            }
                            j = j + 1
                        }
                        val scoredVariants = variants.map(v => v match { case (flexion, lemme, tags, afterTag, beforeTag) => 
                            (tags, lemme,
                                ({
                                    val scoreBefore = prevTags match { case Some(v) => v.cosineSimilarity(beforeTag) case _ => -1.0}
                                    val scoreAfter = nextTags match { case Some(s) if s.size>0 => s.map(v => v.cosineSimilarity(afterTag)).max case _ => -1.0}
                                    val matchScore = if(flexion == word) 0.3 else 0.0
                                    scoreBefore + scoreAfter
                                })
                            )
                        })
                        val (bestTags, bestLemme, bestScore) = scoredVariants.sortWith((p1, p2) => (p1, p2) match {case ((tags1, lemme1, score1), (tags2, lemme2, score2)) => score1 > score2}).head
                        prevTags = Some(bestTags)
                        (word, bestLemme, index,phraseId,simplified,isWord, Some(bestTags))
                    }
                }})
            })})
            .toDF("docId", "words")
            .as[(Int, Seq[(String, String,Int, Int, String, Boolean, SemanticVector)])]
            /*.flatMap(doc => doc match {case (docId, words) => words.map(w => w match {case (word, lemme, index,phraseId,simplified,isWord, tags)=>
                                                                                Word(word = word, simplified = simplified, isWord = isWord, index = index, phraseId = phraseId, docId = docId, root = lemme
                                                                                            , tags = tags match {case Some(v) => v.coord.map(c => c.index.toShort).toArray case _ => Array[Short]()}
                                                                                            , wordType = tags match {case Some(v) => Word.getSemanticUsage(v.coord.map(c => c.index.toShort).toArray) case _ => null.asInstanceOf[String]}
                                                                                )}
            )}).write.mode("overwrite").parquet(taggedWordsPath)
*/


    }
    
/*    def addSemantic(simplifySemantic:Boolean = true
                      , wordTypeScope:Map[String, Double] = Map("Characteristic"->1.0/ *, "Arity"* /,"Quantity"->1.0/ *,"Connector"* /, "Entity"->1.0, "Autre"->1.0, "Negation"->1.0, "Action"->1.0,"Quality"->1.0)
                      , keepSymbols:Boolean = false, keepUnknownWords:Boolean = true, UnknownWordsMinFrequency:Int = 20, stopWords:Seq[String]=Seq[String]()) {
        import spark.implicits._

        val wordVectors = spark.read.parquet(this.semanticVectorsPath).as[SemanticVector]
        val wordsInDocs = spark.read.parquet(this.taggedWordsPath).as[Word]

        val joined = (
            if(simplifySemantic)
                wordsInDocs.joinWith(wordVectors,  $"_1.isWord" && lower($"_1.root") === lower($"_2.word"), "left")
            else 
                wordsInDocs.joinWith(wordVectors, $"_1.isWord" && lower($"_1.word") === lower($"_2.word"), "left")
        )
        val semPhrases = joined.map(p => p match { case(word, vector) => 
			         word.setSemantic(
                                     semantic = if(wordTypeScope.getOrElse(word.wordType, 0.0) <= 0.0 
                                                    || stopWords.filter(s => s == word.root).size > 0
                                                    || vector == null
                                                ) 
							null 
						else 
							vector.scale(wordTypeScope(word.wordType))
                                    ,defaultIfNull = (!word.isWord && keepSymbols) 
                                                   || wordTypeScope.getOrElse(word.wordType, 0.0) > 0 
                                                   || stopWords.filter(s => s == word.root).size == 0
                                    , splitOnCharacters = !word.isWord && keepSymbols
            )})
            .map(w => w.toSemanticPhrase())
            .groupByKey(ps => (ps.docId, ps.phraseId))
            .reduceGroups((sp1, sp2) => sp1.mergeWith(sp2))
            .map(p => p._2)
            .cache
        val sumVector = semPhrases.flatMap(p=>p.phraseSemantic).reduce((s1, s2)=>s1.sum(s2))
        //println(sumVector.coord.map(c => c.value))
        val countVect = semPhrases
                          .flatMap(p=>p.phraseSemantic)
                          .map(s => SemanticVector(word = s.word, coord = s.coord.map(c => Coordinate(index = c.index, value = 1))))
                          .reduce((s1, s2)=>s1.sum(s2))
        //println(countVect..map(c => c.value))

        val indexToRemove = countVect.coord.filter(c => c.value < UnknownWordsMinFrequency && c.index > 300).map(c => c.index).toSet
        val avgVector = countVect.coord.zipWithIndex.map(p => p match {case (count, i) => (count.index, Math.abs(sumVector.coord(i).value)/count.value)}).toMap
        //println(avgVector)
        val w2VAvgSize = avgVector.values.slice(0, 300).reduce(_ + _) / 300

        semPhrases
          .map(s => SemanticPhrase(docId = s.docId, phraseId = s.phraseId, words= s.words, clusterId = s.clusterId
                        , centerDistance = s.centerDistance, hierarchy = s.hierarchy
                        , phraseSemantic = s.phraseSemantic match {
                                                               case None => None 
                                                               case Some(s) => 
                                                                  Some(SemanticVector(
                                                                         s.word
                                                                         , s.coord.map(c => Coordinate(c.index, 
                                                                                                       if(c.index < 300) c.value 
                                                                                                       else 0.5 * c.value * (w2VAvgSize/avgVector(c.index))
                                                                                                        )
                                                                                   )
                                                                                   .filter(c => !indexToRemove.contains(c.index))
                                                              ))}
                     )
          )
          .write.mode("Overwrite").parquet(this.semanticPhrasesPath)
        semPhrases.unpersist
    }
*/
}

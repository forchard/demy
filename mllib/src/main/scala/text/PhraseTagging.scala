package demy.mllib.text

import demy.mllib.linalg.SemanticVector
import demy.mllib.linalg.Coordinate
import demy.mllib.util.util
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}


case class PhraseTagging(textSource:org.apache.spark.sql.DataFrame, commentColumnName:String, lexiquePath:String, semanticVectorsPath: String, taggedWordsPath:String, semanticPhrasesPath: String, spark:org.apache.spark.sql.SparkSession) {
    def tagWords() {
        //TODO: Improve tagging by restarting on sentence break (both on tagging and frequency evaluation)
    
        import org.apache.spark.sql.expressions.{Window}
        import spark.implicits._
        import org.apache.spark.sql.functions._
        
        val lexique_base = this.spark.read
            .option("header", "true")
            .option("sep", "\t")
            .option("nullValue", "*")
            .option("inferSchema", "true")
            .csv(this.lexiquePath)
            .select($"id", $"Flexion", $"Lemme",$"Sémantique",$"Étymologie", $"Étiquettes",$"Fréquence")
        
        val lexique_contractions =    lexique_base
            .where("Flexion = 'je' or (Flexion = 'la' and `Étiquettes` like '%properobj%') or Flexion ='ne' or Flexion ='hôpital'")
            .withColumn("newFlexion", expr("case when Flexion = 'je' then 'j' when Flexion = 'la' then 'l' when Flexion ='ne' then 'n' when Flexion ='hôpital' then 'chu' end"))
            .withColumn("newEtiq", expr("case when Flexion in ('la') then regexp_replace(`Étiquettes`, 'fem ', '') else `Étiquettes` end"))
            .select($"id", $"newFlexion".as("Flexion"),$"Lemme", $"Sémantique", $"Étymologie", $"newEtiq".as("Étiquettes"),$"Fréquence")
        
        val lexique = lexique_base
            .union(lexique_contractions)
            .withColumn("Simplified", Word.sqlSimplify($"Flexion"))
            .withColumn("tags", GramTag.sqlGetGramTag($"Étiquettes", expr("case when Lemme in ('être', 'avoir', 'aller', 'devoir', 'falloir', 'faire', 'pouvoir') then true else false end")))
            .withColumn("rn", row_number().over(Window.partitionBy($"Flexion", $"tags").orderBy($"id")))
            .where($"rn"===1)
            .select($"id", $"Simplified", $"Flexion", $"Lemme",$"Sémantique",$"Étymologie",$"tags", $"Fréquence".as("globalFreq")).as[LexiqueEntry]
            .map(e => GroupedLexiqueEntry(e.id, e.Simplified, Array(e), 1, 1))
            .groupByKey(ge => ge.Simplified)
            .reduceGroups((g1, g2)=>g1.reduceWith(g2))
            .map(p => p._2) //.removeUnusual
            .cache
            
        var aTagged_verbatim_0 = this.textSource.withColumnRenamed(commentColumnName, "comment")
            .select($"comment".as("text"), row_number().over(Window.orderBy($"comment".desc)).as("docId")).as[Doc]
            .flatMap(doc => Word.splitDoc(doc))
            .joinWith(lexique, $"_2.Simplified"===$"_1.simplified", "left")
            .map(p => WordLexOptions(p._1, if(p._2 != null) p._2.disambiguate(p._1.word) else null).applyTag(null, Some("Single Lexique Entry")))
            .map(wo => VerbText(wo.word.docId, Array(wo)))
            .groupByKey( v => v.docId)
            .reduceGroups((v1, v2) => v1.reduceWords(v2))
            .map(p => p._2)
    
        var aFrequences = aTagged_verbatim_0
            .flatMap(v => v.getTransitions)
            .groupByKey(t => (t.from, t.to))
            .reduceGroups((t1, t2) => TagTransition(t1.from, t1.to, t1.count + t2.count, 0, 0.0))
            .map(p => p._2)
            .select($"from", $"to", $"count", max($"count").over(Window.partitionBy($"from")).cast("int").as("fromCount"))
            .withColumn("likehood", (lit(1.0)*$"count")/$"fromCount").as[TagTransition]

        var aFrequences_Right = aFrequences
            .map(tt => tt :: Nil)
            .groupByKey(att => att.head.from.mkString("-"))
            .reduceGroups((l1, l2) => l1 ++ l2)
            .collect
            .toMap
        //Tagging to the words to the left & right if previous word is tagged    
        var aFrequences_Left = aFrequences
            .map(tt => tt :: Nil)
            .groupByKey(att => att.head.to.mkString("-"))
            .reduceGroups((l1, l2) => l1 ++ l2)
            .collect
            .toMap
        
        val aTagged_verbatim_1 = aTagged_verbatim_0
            .map(v => v.tagWords(aFrequences_Right, "right"))
            .map(v => v.tagWords(aFrequences_Left, "left"))
            .map(v => v.setPhrases)
            
        aTagged_verbatim_1
            .flatMap(v => v.words)
            .map(lw => lw.setHumanReadableTags)
            .map(lw => lw.tagAny)
            .map(lw => lw.getTaggedWord)
            .write.mode("Overwrite").parquet(this.taggedWordsPath)

        lexique.unpersist
    }
    
    def addSemantic(simplifySemantic:Boolean = true
                      , wordTypeScope:Seq[String] = Seq("Characteristic"/*, "Arity"*/,"Quantity"/*,"Connector"*/, "Entity", "Autre", "Negation", "Action","Quality")
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
        val semPhrases = joined.map(p => p._1.setSemantic(
                                semantic = if(wordTypeScope.filter(s => p._1.wordType == s).length==0 || stopWords.filter(s => s == p._1.root).size > 0) null else p._2
                                ,defaultIfNull = (!p._1.isWord && keepSymbols) 
                                                   || (wordTypeScope.filter(s => p._1.wordType == s).length>0) 
                                                   || stopWords.filter(s => s == p._1.root).size == 0
                                , splitOnCharacters = !p._1.isWord && keepSymbols
            ))
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
        val avgVector = countVect.coord.zipWithIndex.map(p => p match {case (count, i) => Math.abs(sumVector.coord(i).value)/count.value})
        //println(avgVector)
        val w2VAvgSize = avgVector.slice(0, 300).reduce(_ + _) / 300

        semPhrases
          .map(s => SemanticPhrase(docId = s.docId, phraseId = s.phraseId, words= s.words, clusterId = s.clusterId
                        , centerDistance = s.centerDistance, hierarchy = s.hierarchy
                        , phraseSemantic = s.phraseSemantic match {
                                                               case None => None 
                                                               case Some(s) => 
                                                                  Some(SemanticVector(
                                                                         s.word
                                                                         , s.coord.zipWithIndex.map(p => 
                                                                                       p match {
                                                                                         case (c, i) => Coordinate(c.index, 
                                                                                                                   if(c.index < 300) c.value 
                                                                                                                   else 0.5 * c.value * (w2VAvgSize/avgVector(i))
                                                                                                        )
                                                                                   })
                                                                                   .filter(c => !indexToRemove.contains(c.index))
                                                              ))}
                     )
          )
          .write.mode("Overwrite").parquet(this.semanticPhrasesPath)
        semPhrases.unpersist
    }
}

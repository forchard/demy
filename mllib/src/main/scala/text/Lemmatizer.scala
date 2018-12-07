package demy.mllib.text

import demy.mllib.linalg.{SemanticVector, Coordinate}
import demy.mllib.index.implicits._
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col, lit}


class Lemmatiser(override val uid: String) extends Transformer {
    final val inputCol = new Param[String](this, "inputCol", "The input column")
    final val outputCol = new Param[String](this, "outputCol", "The new column column")
    final val lexiconPath = new Param[String](this, "lexiconPath", "The lexyque parquet file")
    final val indexPath = new Param[String](this, "indexPath", "The HDFS index path to build")
    final val reuseIndexFile = new Param[Boolean](this, "reuseIndexFile", "The workers temporrary directory to copy index file")
    final val rowChunkSize = new Param[Int](this, "rowChunkSize", "The chunk size for querying the lucene index")
    final val indexParallelismLevel = new Param[Int](this, "indexParallelismLevel", "The number of threads to use for reading the index")
    final val simplifyTokens = new Param[Boolean](this, "simplifyTokens", "Remove accents and spécial characters from tokens")
    final val stemTokens = new Param[Boolean](this, "stemTokens", "Remove common endings to match verbs and adjectives with same root")
//    final val maxLevDistance = new Param[Int](this, "maxLevDistance", "The max Levenshtein distance to consider a match against the lexique")
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    def setLexiconPath(value: String): this.type = set(lexiconPath, value)
    def setIndexPath(value: String): this.type = set(indexPath, value)
    def setReuseIndexFile(value: Boolean): this.type = set(reuseIndexFile, value)
    def setRowChunkSize(value:Int):this.type = set(rowChunkSize, value)
    def setIndexParallelismLevel(value:Int):this.type = set(indexParallelismLevel, value)
    def setStemTokens(value:Boolean):this.type = set(stemTokens, value)
    def setSimplifyTokens(value:Boolean):this.type = set(simplifyTokens, value)
    setDefault(rowChunkSize -> 1000, indexParallelismLevel -> 2, simplifyTokens -> false, stemTokens -> false)
//    def setMaxLevDistance(value: Int): this.type = set(maxLevDistance, value)
    override def transform(dataset: Dataset[_]): DataFrame =
        dataset
          .luceneLookup(
            right = dataset.sparkSession.read.parquet(get(lexiconPath).get)
            , query = udf((tokens:Seq[String])=> tokens.map(w => Word.simplifyText(w))).apply(col(get(inputCol).get))
            , text= col("simplified")
            , maxLevDistance=0
            , indexPath=get(indexPath).get
            , reuseExistingIndex=get(reuseIndexFile).get
            , leftSelect=Array(col("*"))
            , rightSelect=Array(col("*"))
            , popularity=None
            , indexPartitions = 1
            , maxRowsInMemory=getOrDefault(rowChunkSize)
            , indexScanParallelism= getOrDefault(indexParallelismLevel)
            , tokenizeText = false)
          .withColumn(get(outputCol).get, udf((words:Seq[String], lexyqueMatchRow:Seq[Row], simplify:Boolean, stem:Boolean) => {
            val lexyqueMatchs = lexyqueMatchRow.map(r => (r.getAs[String](0), r.getAs[Seq[String]](1), r.getAs[Seq[String]](2), r.getAs[Seq[Seq[Double]]](3) match {case v => if(v==null) Seq[Seq[Double]]() else v}, r.getAs[Seq[Seq[Double]]](4) match {case v => if(v==null) Seq[Seq[Double]]() else v}, r.getAs[Seq[Seq[Double]]](5) match {case v => if(v==null) Seq[Seq[Double]]() else v}))
            val toSteam = Seq("er"->"", "ereux"->"", "se"->"s", "me"->"m", "ne"->"n", "aux"->"", "re"->"r", "ge"->"g", "e"-> "", "te"-> "t", "ion"->""
                             , "ce"->"c", "eu"->"", "au"->"", "ement"->"", "ence"->""
                             , "innation" -> "in", "atoire"-> "", "gacion"->"", "itoire"-> "")
            val defTags = Range(0, GramTag.Nom.id + 1).map(i => if(i == GramTag.Nom.id) 1.0 else 0.0).toSeq
            var prevTags:Option[Seq[Double]] = None
            words.zipWithIndex
              .map(t => t match {case (word, i) => {
                val (flexions, lemmes, tags, forwardVectors, backwardsVectors) = lexyqueMatchs(i) match {case (simplified, flexions, lemmes, tags, forwardVectors, backwardsVectors) => (flexions, lemmes, tags, forwardVectors, backwardsVectors)}
                if(tags.filter(t => t.size > 0).size == 0){
                    prevTags = Some(defTags)
                    word
                } else {
                    var nextTags = if(i + 1 < words.size && (lexyqueMatchs(i + 1) match {case (nSimplified, nFlexions, nLemmes, nTags, nForwardVector, nBackwardsVector) => nTags}).filter(tag => tag != null && tag.size > 0).size > 0)
                                    Some(lexyqueMatchs(i + 1) match {case (nSimplified, nFlexions, nLemmes, nTags, nForwardVector, nBackwardsVector) => nTags})
                                else
                                    None
                                    
                    val scoredVariants = tags.zipWithIndex.map(p => p match { case (tag, j) =>
                        (tag, lemmes(j),
                            ({
                                val scoreBefore = prevTags match { case Some(v) => v.cosineSimilarity(backwardsVectors(j)) case _ => -1.0}
                                val scoreAfter = nextTags match { case Some(s) if s.size>0 => s.map(v => v.cosineSimilarity(forwardVectors(j))).max case _ => -1.0}
                                val matchScore = if(flexions(j) == word) 0.3 else 0.0
                                scoreBefore + scoreAfter
                            })
                        )
                    })
                    val (bestTag, bestLemme, bestScore) = scoredVariants.sortWith((t1, t2) => (t1, t2) match {case ((tags1, lemme1, score1), (tags2, lemme2, score2)) => score1 > score2}).head
                    prevTags = Some(bestTag)
                    bestLemme
                }
              }})
              .map(token => if(simplify) Word.simplifyText(token) else token )
              .map(token => if(stem && token.size > 2) toSteam.reverse.filter(s => token.endsWith(s._1) && token.size - s._1.size >2).take(1) match {case Seq(s) => token.substring(0, token.size - s._1.size)+s._2 case _ => token} else token)
             }).apply(col(get(inputCol).get), col("array"), lit(getOrDefault(simplifyTokens)), lit(getOrDefault(stemTokens)))
          )
          .drop("array")

    override def transformSchema(schema: StructType): StructType = {schema.add(StructField(get(outputCol).get, ArrayType(elementType=StringType, containsNull = true)))}
    def copy(extra: ParamMap): Lemmatiser = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("lemmatizer"))
}

object Lemmatiser {
}

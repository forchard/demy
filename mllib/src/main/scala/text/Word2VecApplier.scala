package demy.mllib.text

import demy.mllib.params.HasExecutionMetrics
import demy.mllib.index.implicits._
import demy.mllib.util.log.msg
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col}
import org.apache.spark.ml.linalg.{Vectors, DenseVector}

class Word2VecApplier(override val uid: String) extends Transformer with HasExecutionMetrics {
    final val inputCol = new Param[String](this, "inputCol", "The input column")
    final val outputCol = new Param[String](this, "outputCol", "The new column column")
    final val format = new Param[String](this, "format", "The vectors format")
    final val vectorsPath = new Param[String](this, "vectorsPath", "The vectors location")
    final val indexPath = new Param[String](this, "indexPath", "A temporary shared path to build the lucene index containing the vectors for map-like lookup")
    final val workersTmp = new Param[String](this, "workersTmp", "A temporary local path to store local index copy")
    final val reuseIndexFile = new Param[Boolean](this, "reuseIndexFile", "If the index file can be reused when already exists")
    final val truncateWordsAt = new Param[Int](this, "truncateWordsAt", "The max number of characters to use on each word to match the vectors")
    final val accentSensitive = new Param[Boolean](this, "accentSensitive", "If accents are to be considered when matching vectors")
    final val caseSensitive = new Param[Boolean](this, "caseSensitive", "If case is to be considered when matching vectors")
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    def setFormat(value: String): this.type = set(format, value)
    def setVectorsPath(value: String): this.type = set(vectorsPath, value)
    def setIndexPath(value: String): this.type = set(indexPath, value)
    def setReuseIndexFile(value: Boolean): this.type = set(reuseIndexFile, value)
    def setWorkersTmp(value: String): this.type = set(workersTmp, value)
    def setTruncateWordsAt(value: Int): this.type = set(truncateWordsAt, value)
    def setAccentSensitive(value: Boolean): this.type = set(accentSensitive, value)
    def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)
    override def transform(dataset: Dataset[_]): DataFrame = {
        val spark = dataset.sparkSession
        import spark.implicits._
        val (wordCount, matchCount) = (spark.sparkContext.longAccumulator(name = "Word2VecApplier_Count"+uid), spark.sparkContext.longAccumulator(name = "Word2VecApplier_Matchs"+uid))
        val vPath = get(vectorsPath).get
        val wordLimit = getOrDefault(truncateWordsAt)
        val simplify = !getOrDefault(accentSensitive)
        val toLower = !getOrDefault(caseSensitive)
        val vectorColName = get(outputCol).get
        val vectorsDF = getOrDefault(format) match {
            case "spark" => spark.read.parquet(vPath).as[(String, Array[Double])].map(p => (p._1, Vectors.dense(p._2))).toDF("__token__", vectorColName)
            case "text" => spark.read.text(vPath).as[String].map(s => s.split(" ")).filter(a => a.size>300).map(a => (a(0), Vectors.dense(a.drop(1).map(s => s.toDouble)))).toDF("__token__", vectorColName)
        }
        
        val ret = dataset
                    .luceneLookup(right = vectorsDF, query = udf((tokens:Seq[String])=> applyCaseAccentsAndLimit(tokens, wordLimit, simplify, toLower)).apply(col(get(inputCol).get)), text=col("__token__"), maxLevDistance=0, indexPath=get(indexPath).get, reuseExistingIndex=get(reuseIndexFile).get, leftSelect=Array(col("*")), rightSelect=Array(col("*")), popularity=None, workersTmpDir=get(workersTmp).get, indexPartitions = 1, maxRowsInMemory=10000, indexScanParallelism= 5, tokenizeText = false)
                    .withColumn(vectorColName, udf((results:Seq[Row], words:Seq[String])=>{
                        if(words.size != results.size) throw new Exception("invalid match @epi @deleteme")
                        wordCount.add(words.size)
                        matchCount.add(results.map(r => r.getAs[DenseVector](vectorColName)).filter(v => v != null).size)
                        results.map(r => r.getAs[DenseVector](vectorColName))
                               .reduce((v1, v2)=>if(v1==null) v2 else if(v2==null) v1 else new DenseVector(v1.values.zip(v2.values).map(p => p._1 + p._2)))
                        }).apply(col("array"), col(get(inputCol).get)))
                    .drop("array")
        if(getLogMetrics) {
          val c = ret.count
          msg(s"calculating Word2Vec hitPercent on $c lines")
          metrics += ("hitPercent" -> matchCount.value.toDouble / wordCount.value.toDouble)
        }
        ret
    }
    
    def applyCaseAccentsAndLimit(tokens:Seq[String], wordLimit:Int, simplify:Boolean, toLower:Boolean) = {
        if(simplify && !toLower) throw new Exception("Non Accent sensitive and case sensitive is not yet supported @epi")
        tokens
            .map(w => if(wordLimit>0) w.slice(0, wordLimit) else w)
            .map(w => if(toLower) w.toLowerCase else w)
            .map(w => if(simplify) Word.simplifyText(w) else w)
    }
    override def transformSchema(schema: StructType): StructType = {schema.add(new AttributeGroup(name=get(outputCol).get).toStructField)}
    def copy(extra: ParamMap): Word2VecApplier = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("Word2VecApplier"))
}
object Word2VecApplier

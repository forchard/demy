package demy.mllib.text

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.shared._

class TweetCleaner(override val uid: String) extends Transformer with HasInputCol with HasOutputCol {
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    override def transform(dataset: Dataset[_]): DataFrame = {
       dataset.withColumn(get(outputCol).get, udf((text:String) => {
         val url = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)"
         val user = "(?<=^|(?<=[^a-zA-Z0-9-_\\.]))@(\\w+)\\b"
         val stop = "\\bRT\\b|\\bvia\b"
         val hash = "#"
         text.replaceAll(url, " ce lien ")
           .replaceAll(user, " toi ")
           .replaceAll(stop, " ")
           .replaceAll(hash, "")
       }).apply(col(get(inputCol).get)))
    }
    override def transformSchema(schema: StructType): StructType = {schema.add(StructField(get(outputCol).get, StringType))}
    def copy(extra: ParamMap): AddId = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("TweetCleaner"))
}
object TweetCleaner {
}

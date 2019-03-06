package demy.mllib.feature

import demy.mllib.params._
import demy.util.{log => l}
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.attribute.AttributeGroup 
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col}

trait Tag2VectorBase extends Params with HasInputCol with HasOutputCol{
    val uid: String
    final val minFreq = new Param[Int](this, "minFreq", "threshold indicating the minimim frequency of a identified as a tag")
    final val topClasses = new Param[Int](this, "topClasses", "maximum number of classes to return ranked by frequency")
    final val trim = new Param[Boolean](this, "trim", "If tags should be trimmed")
    final val caseSensitive = new Param[Boolean](this, "caseSensitive", "If case is to be taken on consideration when identifying distincts tags")
    def setTrim(value: Boolean): this.type = set(trim, value)
    def setCaseSensitive(value: Boolean): this.type = set(caseSensitive, value)
    def setMinFreq(value: Int): this.type = set(minFreq, value)
    def setTopClasses(value: Int): this.type = set(topClasses, value)
    setDefault(trim -> true, caseSensitive -> false, minFreq -> 0, topClasses -> 0)
    def validateAndTransformSchema(schema: StructType): StructType = {schema.add(new AttributeGroup(name=get(outputCol).get).toStructField)}
}
object Tag2VectorBase {
}
class Tag2VectorModel(override val uid: String, val dictionary:Map[String, Int]) 
  extends org.apache.spark.ml.Model[Tag2VectorModel] with Tag2VectorBase
{
  override def transform(dataset: Dataset[_]): DataFrame = {
    val doTrim = getOrDefault(trim)
    val doToLower = !getOrDefault(caseSensitive)

    dataset.withColumn(get(outputCol).get, udf((values:Seq[String])=>{
      val cValues = if(values==null) Seq[String]() else values 
      val indices = cValues.flatMap{v =>
        var tag = if(doTrim) v.trim else v  
        tag = if(doToLower) tag.toLowerCase else tag  
        if(dictionary.contains(tag)) Some(dictionary(tag)) 
        else None
      }
      .distinct
      .sortWith(_ < _)
      .toArray
      
      new SparseVector(size=dictionary.size, indices= indices, values= indices.map(i => 1.0))   
    }).apply(col(get(inputCol).get)))
  }
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
  def copy(extra: ParamMap): Tag2VectorModel = {defaultCopy(extra)}    
  def this() = this(Identifiable.randomUID("tag2VectorModel"), Map[String, Int]())
}

class Tag2Vector(override val uid: String) extends Estimator[Tag2VectorModel] with Tag2VectorBase{
  override def fit(dataset: Dataset[_]): Tag2VectorModel = {
    import dataset.sparkSession.implicits._
    val doTrim = getOrDefault(trim)
    val doToLower = !getOrDefault(caseSensitive)
    val minF = getOrDefault(minFreq)
    val topC = getOrDefault(topClasses)
    val sorted = dataset
      .select(col(get(inputCol).get)).as[Seq[String]]
      .flatMap{a => 
        if(a==null)
          Seq[(String, Long)]()
        else a.flatMap{ v =>
          var tag = if(v == null) "" else v
          tag = if(doTrim) tag.trim else tag  
          tag = if(doToLower) tag.toLowerCase else tag
          if(tag == "") None else Some(tag, 1L)
        }
      }
      .groupByKey{case (tag, count)=> tag}
      .reduceGroups((p1, p2)=> (p1, p2) match {
        case ((tag, count1), (_, count2)) => (tag, count1 + count2)
      })
      .flatMap{case (_, (tag, count)) => if(count >= minF) Some(tag, count) else None}
      .sort(col("_2").desc)
      
    val dico = (if(topC>0) sorted.take(topC) else sorted.collect)
      .map{case (tag, count) => tag}
      .zipWithIndex
      .toMap
       
    l.msg(s"Dictionnary with ${dico.size} entries: $dico")
    copyValues(new Tag2VectorModel(uid, dico).setParent(this))
  }
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
  def copy(extra: ParamMap): Tag2Vector = {defaultCopy(extra)}    
  def this() = this(Identifiable.randomUID("tag2Vector"))
}

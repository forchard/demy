package demy.mllib.feature

import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.attribute.AttributeGroup 
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col}

trait Tag2VectorBase extends Params {
    val uid: String
    val dictionary:Map[String, Int] 
    final val inputCol = new Param[String](this, "inputCol", "The input column it sould be an array of strings")
    final val outputCol = new Param[String](this, "outputCol", "The new column")
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    def validateAndTransformSchema(schema: StructType): StructType = {schema.add(new AttributeGroup(name=get(outputCol).get).toStructField)}
}
object Tag2VectorBase {
}
class Tag2VectorModel(override val uid: String, val dictionary:Map[String, Int]) extends org.apache.spark.ml.Model[Tag2VectorModel] with Tag2VectorBase{
    override def transform(dataset: Dataset[_]): DataFrame = {
            dataset.withColumn(get(outputCol).get, udf((values:Seq[String])=>{
            val indices = values.map(v => if(dictionary.contains(v)) dictionary(v) else throw new Exception("Value not found on Tag2Vector dictionary")).distinct.sortWith(_ < _).toArray
            new SparseVector(size=dictionary.size, indices= indices, values= indices.map(i => 1.0))   
        }).apply(col(get(inputCol).get)))
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): Tag2VectorModel = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("tag2VectorModel"), Map[String, Int]())
}
object Tag2Vector {
}
class Tag2Vector(override val uid: String, val dictionary:Map[String, Int]) extends Estimator[Tag2VectorModel] with Tag2VectorBase{
    override def fit(dataset: Dataset[_]): Tag2VectorModel = {
      import dataset.sparkSession.implicits._
      val dico = dataset.select(col(get(inputCol).get)).as[Seq[String]].flatMap(a => a).distinct.collect.sortWith(_ < _).zipWithIndex.toMap
      new Tag2VectorModel(uid, dico).setInputCol(get(inputCol).get).setOutputCol(get(outputCol).get).setParent(this)
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): Tag2Vector = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("tag2Vector"), Map[String, Int]())
}
object Tag2VectorModel {
}

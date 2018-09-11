package demy.mllib.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{Identifiable, DefaultParamsWritable, DefaultParamsReadable}
import org.apache.spark.ml.linalg.SparseVector
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col}
import scala.util.hashing.MurmurHash3
import org.apache.spark.ml.attribute.AttributeGroup 

class ArrayHasher(override val uid: String) extends Transformer with DefaultParamsWritable {
    final val inputCol = new Param[String](this, "inputCol", "The input column")
    final val outputCol = new Param[String](this, "outputCol", "The new column column")
    final val numFeatures = new Param[Int](this, "numFeatures", "The number of components on outut vectors")
    def setInputCol(value: String): this.type = set(inputCol, value)
    def setOutputCol(value: String): this.type = set(outputCol, value)
    def setNumFeatures(value: Int): this.type = set(numFeatures, value)
    override def transform(dataset: Dataset[_]): DataFrame = {
        dataset.withColumn(get(outputCol).get, udf((words:Seq[String])=>{
        val size =  get(numFeatures).get
        val pairs = words.map(w => (MurmurHash3.stringHash(w, MurmurHash3.stringSeed) % size)  match {case x => if(x < 0) -x else x})
            .groupBy(id => id)
            .map(p => p match {case (id, ids) => (id, ids.size)})
            .toArray.sortWith((p1, p2) => (p1, p2) match {case ((id1, count1),(id2, count2)) => id1 < id2})
    
        new SparseVector(size=size, indices= pairs.map(p => p._1), values= pairs.map(p => p._2.toDouble)) 
    }).apply(col(get(inputCol).get)))
    }
    override def transformSchema(schema: StructType): StructType = {schema.add(new AttributeGroup(name=get(outputCol).get).toStructField)}
    def copy(extra: ParamMap): ArrayHasher = { defaultCopy(extra) }    
    def this() = this(Identifiable.randomUID("ArrayHasher"))
}
object ArrayHasher extends DefaultParamsReadable[ArrayHasher]{
}

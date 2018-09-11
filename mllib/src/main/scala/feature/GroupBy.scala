package demy.mllib.feature

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.linalg.{Vectors, DenseVector, SparseVector}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._

class GroupBy(override val uid: String) extends Transformer {
    final val maxCols = new Param[Array[String]](this, "maxCols", "The column to reduce by using max")
    final val minCols = new Param[Array[String]](this, "minCols", "The column to reduce by using min")
    final val sumCols = new Param[Array[String]](this, "sumCols", "The column to reduce by using sum")
    final val groupByCols = new Param[Array[String]](this, "groupByCols", "Columns to group by")
    final val peekCols = new Param[Array[String]](this, "peekCols", "Columns to reduce by choosing any of the ekements in the group")
    final val peekRest = new Param[Boolean](this, "peekRest", "If unespecified columns should be reduced by choosing any of the ekements in the group")
    def setMaxCols(value: Array[String]): this.type = set(maxCols, value)
    def setMinCols(value: Array[String]): this.type = set(minCols, value)
    def setSumCols(value: Array[String]): this.type = set(sumCols, value)
    def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)
    def setPeekCols(value: Array[String]): this.type = set(peekCols, value)
    def setPeekRest(value: Boolean): this.type = set(peekRest, value)
    setDefault(maxCols -> Array[String](), minCols -> Array[String](), sumCols -> Array[String](), peekCols -> Array[String](), peekRest -> true)
    override def transform(dataset: Dataset[_]): DataFrame = {
        val df = dataset.toDF
        val schema = df.schema
        val actions = indexAction(schema)
        val inFieldsCount = schema.fields.size
        val groupBy = get(groupByCols).get
        val groupByIndexes = groupBy.map(c => schema.fields.map(f => f.name).indexOf(c))
        val rdd = df.rdd.map(r => (groupByIndexes.map(i => r.apply(i)).toSeq, r))
              .reduceByKey((r1, r2) => Row.fromSeq(Range(0, inFieldsCount).toSeq.map(i => actions(i) match {
                  case Some("max") => (r1.apply(i), r2.apply(i)) match {
                      case (v1:Float, v2:Float) => if(v1 > v2) v1 else v2
                      case (v1:Int, v2:Int) => if(v1 > v2) v1 else v2
                      case (v1:Double, v2:Double) => if(v1 > v2) v1 else v2
                      case (v1:String, v2:String) => if(v1 > v2) v1 else v2
                      case (v1:DenseVector, v2:DenseVector) => Vectors.dense(v1.values.zip(v2.values).map(p => maxReducer(p._1, p._2)))
                      case (v1:SparseVector, v2:SparseVector) => applyToSparseVector(v1, v2, maxReducer)
                  }
                  case Some("min") => (r1.apply(i), r2.apply(i)) match {
                      case (v1:Float, v2:Float) => if(v1 < v2) v1 else v2
                      case (v1:Int, v2:Int) => if(v1 < v2) v1 else v2
                      case (v1:Double, v2:Double) => if(v1 < v2) v1 else v2
                      case (v1:String, v2:String) => if(v1 < v2) v1 else v2
                      case (v1:DenseVector, v2:DenseVector) => Vectors.dense(v1.values.zip(v2.values).map(p => minReducer(p._1, p._2)))
                      case (v1:SparseVector, v2:SparseVector) => applyToSparseVector(v1, v2, minReducer)
                  }
                  case Some("sum") => (r1.apply(i), r2.apply(i)) match {
                      case (v1:Float, v2:Float) => v1 + v2
                      case (v1:Int, v2:Int) => v1 + v2
                      case (v1:Double, v2:Double) => v1 + v2
                      case (v1:String, v2:String) => throw new Exception("Cannot add strings")
                      case (v1:DenseVector, v2:DenseVector) => Vectors.dense(v1.values.zip(v2.values).map(p => sumReducer(p._1, p._2)))
                      case (v1:SparseVector, v2:SparseVector) => applyToSparseVector(v1, v2, sumReducer)
                  }
                  case Some("peek") => r1.apply(i)
                  case Some("group") => r1.apply(i)
                  case _ => null
              })))
              .map(p => Row.fromSeq(Range(0, inFieldsCount).toSeq.flatMap(i => actions(i) match {case Some(s) => Some(p._2.apply(i)) case _ => None})))
        dataset.sparkSession.createDataFrame(rdd, transformSchema(schema)) 
    }
    def indexAction(schema:StructType) = {
        schema.map(f => {
            if(getOrDefault(maxCols).contains(f.name)) Some("max")
            else if(getOrDefault(minCols).contains(f.name)) Some("min")
            else if(getOrDefault(sumCols).contains(f.name)) Some("sum")
            else if(getOrDefault(peekCols).contains(f.name)) Some("peek")
            else if(getOrDefault(groupByCols).contains(f.name)) Some("group")
            else if(getOrDefault(peekRest)) Some("peek")
            else None
        })
    }

    override def transformSchema(schema: StructType): StructType = {
        val keepRest = get(peekRest).get
        if(keepRest) schema
        else new StructType(schema.fields.filterNot(f => !getOrDefault(groupByCols).contains(f.name) && !getOrDefault(peekCols).contains(f.name) && !getOrDefault(maxCols).contains(f.name) && !getOrDefault(minCols).contains(f.name)  && !getOrDefault(sumCols).contains(f.name)))
    }
    def applyToSparseVector(v1:SparseVector, v2:SparseVector, reduce:(Double, Double)=>Double) = {
        val newIndexes = scala.collection.mutable.ArrayBuffer[Int]()
        val newValues = scala.collection.mutable.ArrayBuffer[Double]()

        var i1 = 0
        var i2 = 0
        while(i1 < v1.indices.size || i2 < v2.indices.size) {
          if(i1<v1.indices.size && (i2>=v2.indices.size || v1.indices(i1)<v2.indices(i2))) {
              val newValue = reduce(v1.values(i1), 0.0)
              if(newValue!=0.0) {
                  newIndexes += v1.indices(i1)
                  newValues += newValue
              }
              i1 = i1 + 1
          } else if(i2<v2.indices.size && (i1>=v1.indices.size || v2.indices(i2)<v1.indices(i1))) {  
              val newValue = reduce(v2.values(i2), 0.0)
              if(newValue!=0.0) {
                  newIndexes += v2.indices(i2)
                  newValues += newValue
              }
              i2 = i2 + 1
          } else {//both arrays are on the same index and have a next element 
              newIndexes += v1.indices(i1)
              newValues +=  reduce(v1.indices(i1), v2.indices(i2))
              i1 = i1 + 1
              i2 = i2 + 1
          }
        }        
    }
    def maxReducer(v1:Double, v2:Double) = if(v1 > v2) v1 else v2
    def minReducer(v1:Double, v2:Double) = if(v1 < v2) v1 else v2
    def sumReducer(v1:Double, v2:Double) = v1 + v2
    def copy(extra: ParamMap): GroupBy = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("groupBy"))
}
object GroupBy {
}

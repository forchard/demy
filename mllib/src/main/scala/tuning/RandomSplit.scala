package demy.mllib.tuning

import demy.mllib.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{hash, col, abs, lit, udf}
import org.apache.spark.sql.ColumnName
import scala.util.Random
import scala.util.hashing.MurmurHash3

class RandomSplit(override val uid: String) extends Folder with HasFolds with HasTrainRatio with HasGroupByCols with HasStratifyByCols with HasSeed{
    
    setDefault(trainRatio->0.75, groupByCols->Array[String](), numFolds -> 1, stratifyByCols -> Array[String]())
    override def buildFolds[T](ds:Dataset[T]):Array[(Dataset[T], Dataset[T])] = {
      val ratio = getOrDefault(trainRatio)
      val groupColumns = getOrDefault(groupByCols)
      val stratColumns = getOrDefault(stratifyByCols)
      val nFolds = getOrDefault(numFolds)
      val probs = if(nFolds <= 1) Array(1 - getOrDefault(trainRatio), getOrDefault(trainRatio)) else Range(0, nFolds).map(_ => 1.0/nFolds).toArray

      if(stratColumns.size > 0) {
        val strats = ds.toDF
            .select(stratColumns.map(c => col(c)) :_*)
            .distinct.collect
                      .map(row => this.setStratifyByCols(Array[String]())
                                      .buildFolds(ds.where(stratColumns.map(colName => col(colName) === lit(row.getAs[Any](colName))).reduce(_ && _))))
        strats.reduce((p1, p2) => p1.zip(p2).map(p => p match {case ((test1, train1),(test2, train2))=> (test1.unionAll(test2), train1.unionAll(train2))})) 
      } else if(groupColumns.size == 0) {
        val splited = ds.randomSplit(probs, getOrDefault(seed))
        Range(0, nFolds).toArray.map(iFold => (splited.zipWithIndex.filter(p => p._2 != iFold).map(p => p._1).reduce((ds1, ds2) => ds1.unionAll(ds2)), splited(iFold)))
      } else {
        val random =new Random(getOrDefault(seed))
        val randInt = random.nextInt
        //println(s"seed is ${get(seed)} and rand is $randInt")
        var p0 = 0.0
        var i = 0
        val pRanges = probs.map(p => {
          val ret = (p0, if(i<nFolds) p0 + p else 1.01 /*to avoind border line case when hashcode == Int.MaxValue*/)
          i = i + 1
          p0 = p0 + p
          ret
        }).take(nFolds)
        val hashExp = abs(hash((groupColumns.map(c => col(c)):_*)))
        pRanges.map(p => p match {case (p0, p1) => 
               (ds.where(udf((rowHash:Double, p0:Double, p1:Double, s:Int )=>{
                          val rowP = Math.abs(MurmurHash3.stringHash(rowHash.toString, s)).toDouble / Int.MaxValue
                          rowP< p0 || rowP>=p1
                         }).apply(hashExp, lit(p0), lit(p1), lit(randInt))
                   )
               ,ds.where(udf((rowHash:Double, p0:Double, p1:Double, s:Int )=>{
                          val rowP = Math.abs(MurmurHash3.stringHash(rowHash.toString, s)).toDouble / Int.MaxValue
                          rowP>= p0 && rowP<p1
                         }).apply(hashExp, lit(p0), lit(p1), lit(randInt))
                   )
               )
        })
        
    }
  }
  def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
  def this() = this(Identifiable.randomUID("RandomSplit"))
}

object RandomSplit{
}

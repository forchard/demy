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

class RandomSplit(override val uid: String) extends Folder with HasFolds with HasTrainRatio with HasGroupByCols with HasStratifyByCols {
    final val seed = new Param[Long](this, "seed", "The random seed")
    def setSeed(value: Long): this.type = set(seed, value)
    
    setDefault(trainRatio->0.75, groupByCols->Array[String](), numFolds -> 1, stratifyByCols -> Array[String]())
    override def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = Array[(Dataset[_], Dataset[_])](typedBuildFolds(ds):_*)
    def typedBuildFolds[T](ds:Dataset[T]):Array[(Dataset[T], Dataset[T])] = {
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
                                      .typedBuildFolds(ds.where(stratColumns.map(colName => col(colName) === lit(row.getAs[Any](colName))).reduce(_ && _))))
        strats.reduce((p1, p2) => p1.zip(p2).map(p => p match {case ((test1, train1),(test2, train2))=> (test1.unionAll(test2), train1.unionAll(train2))})) 
      } else if(groupColumns.size == 0) {
        val splited = get(seed) match { case Some(s) => ds.randomSplit(probs, s) case _ => ds.randomSplit(probs) }
        Range(0, nFolds).toArray.map(iFold => (splited.zipWithIndex.filter(p => p._2 != iFold).map(p => p._1).reduce((ds1, ds2) => ds1.unionAll(ds2)), splited(iFold)))
      } else {
        val random = get(seed) match {case Some(s) => new Random(s) case _ => new Random()}
        val randInt = random.nextInt
          //.match {case v => if(v<0) -v else v }) / Int.MaxValue.toDouble
        var p0 = 0.0
        var i = 0
        val pRanges = probs.map(p => {
          val ret = (p0, if(i<nFolds) p0 + p else 1.01 /*to avoind border line case when hashcode == Int.MaxValue*/)
          i = i + 1
          p0 = p0 + p
          ret
        }).take(nFolds)
        val hashExp = abs(hash((groupColumns.map(c => col(c)) :+ lit(randInt):_*))) / lit(Int.MaxValue)
        pRanges.map(p => p match {case (p0, p1) => 
               (ds.where(udf((rowHash:Double, p0:Double, p1:Double )=>{
                          rowHash< p0 || rowHash>=p1
                         }).apply(hashExp, lit(p0), lit(p1))
                   )
               ,ds.where(udf((rowHash:Double, p0:Double, p1:Double )=>{
                          rowHash>= p0 && rowHash<p1
                         }).apply(hashExp, lit(p0), lit(p1))
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

package demy.mllib.tuning

import demy.mllib.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{hash, col, abs, lit}
import org.apache.spark.sql.ColumnName
import scala.util.Random

class RandomSplit(override val uid: String, val seed:Int) extends Folder with HasFolds with HasTrainRatio with HasGroupByCols{
    val random = new Random(seed)
    setDefault(trainRatio->0.75, groupByCols->Array[String](), numFolds -> 1)
    override def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = {
      val ratio = getOrDefault(trainRatio)
      val groupColumns = getOrDefault(groupByCols)
      val nFolds = getOrDefault(numFolds)
      val probs = if(nFolds <= 1) Array(1 - getOrDefault(trainRatio), getOrDefault(trainRatio)) else Range(0, nFolds).map(_ => 1.0/nFolds).toArray

      if(groupColumns.size == 0) {
        val splited = ds.randomSplit(probs)
        Range(0, nFolds).toArray.map(iFold => (splited.zipWithIndex.filter(p => p._2 != iFold).map(p => p._1).reduce((ds1, ds2) => ds1.unionAll(ds2)), splited(iFold)))
      } else {
        val randInt = random.nextInt
          //.match {case v => if(v<0) -v else v }) / Int.MaxValue.toDouble
        var p0 = 0.0
        var i = 0
        val pRanges = probs.map(p => {
          i = i + 1
          val ret = (p0, if(i<nFolds) p0 + p else 1.01 /*to avoind border line case when hashcode == Int.MaxValue*/)
          p0 = p0 + p
          ret
        })
        val hashCol = "_hash"+this.uid
        val withHash = ds.withColumn(hashCol, abs(hash((groupColumns.map(c => col(c)) :+ lit(randInt):_*))) / lit(Int.MaxValue))
        pRanges.map(p => p match {case (p0, p1) => 
             (withHash.where(col(hashCol)< lit(p0) || col(hashCol)>=p1) 
               ,withHash.where(col(hashCol)>= lit(p0) && col(hashCol)<p1)
               )
        })
    }
  }
  def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
  def this(uid:String) = this(Identifiable.randomUID("RandomSplit"), new scala.util.Random().nextInt)
  def this() = this(Identifiable.randomUID("RandomSplit"))
}

object RandomSplit{
}

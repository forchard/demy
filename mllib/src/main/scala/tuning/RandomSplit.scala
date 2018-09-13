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
    setDefault(trainRatio->0.75, groupByCols->Array[String]())
    override def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = {
      val ratio = getOrDefault(trainRatio)
      val groupColumns = getOrDefault(groupByCols)
      val Array(train, test) = 
        if(groupColumns.size == 0)
          ds.randomSplit(Array(ratio, 1.0-ratio))
        else {
          val randInt = random.nextInt
            //.match {case v => if(v<0) -v else v }) / Int.MaxValue.toDouble
            Array(ds.where(abs(hash((groupColumns.map(c => col(c)) :+ lit(randInt):_*))) / lit(Int.MaxValue) <= lit(ratio))
              ,ds.where(abs(hash((groupColumns.map(c => col(c)) :+ lit(randInt):_*))) / lit(Int.MaxValue) > lit(ratio)))

       }
      Array((train, test))
    }
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this(uid:String) = this(Identifiable.randomUID("RandomSplit"), new scala.util.Random().nextInt)
    def this() = this(Identifiable.randomUID("RandomSplit"))
}

object RandomSplit{
}

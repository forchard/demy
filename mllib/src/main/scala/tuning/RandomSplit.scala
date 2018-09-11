package demy.mllib.tuning

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._

class RandomSplit(override val uid: String) extends Folder {
    final val trainRatio = new Param[Double](this, "trainRatio", "The train set ratio as a proportion e.g. 0.75 meand that 3/4 of randomly selected rows will be used for training")
    def setTrainRatio(value: Double): this.type = set(trainRatio, value)
    override def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = {
      val ratio = get(trainRatio).get
      val Array(train, test) = ds.randomSplit(Array(ratio, 1.0-ratio))
      Array((train, test))
    }
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("RandomSplit"))
}

object RandomSplit

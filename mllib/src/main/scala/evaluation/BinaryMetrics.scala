package demy.mllib.evaluation

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._

case class BinaryMetrics(threshold:Option[Double]=None, tp:Option[Int], tn:Option[Int], fp:Option[Int], fn:Option[Int]
                            , basePrecision:Option[Double]=None, precision:Option[Double]=None, baseRecall:Option[Double]=None, recall:Option[Double]=None, baseF1Score:Option[Double]
                            , f1Score:Option[Double]=None, areaUnderROC:Option[Double]=None, rocCourve:Array[(Double, Double)]=Array[(Double, Double)]()) {
}
trait HasBinaryMetrics {
    val metrics:BinaryMetrics
}

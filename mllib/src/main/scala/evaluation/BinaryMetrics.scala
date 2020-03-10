package demy.mllib.evaluation

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Param
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._

case class BinaryMetrics(
  threshold:Option[Double]=None
  , tp:Option[Int], tn:Option[Int], fp:Option[Int], fn:Option[Int]
//  , baseAccuracy:Option[Double]=None, accuracy:Option[Double]=None
  , basePrecision:Option[Double]=None, precision:Option[Double]=None
  , baseRecall:Option[Double]=None, recall:Option[Double]=None
  , baseF1Score:Option[Double], f1Score:Option[Double]=None
  , areaUnderROC:Option[Double]=None, rocCurve:Array[(Double, Double)]=Array[(Double, Double)]()
  , accuracy:Option[Double]=None, pValue:Option[Double]=None
) {
  lazy val report =
    s"""
    threshold: ${threshold}
    tp: ${tp}
    tn: ${tn}
    fp: ${fp}
    fn: ${fn}
    basePrecision: ${basePrecision}
    Precision: ${precision}
    baseRecall: ${baseRecall}
    recall: ${recall}
    baseF1: ${baseF1Score}
    F1: ${f1Score}
    areaUnderROC: ${areaUnderROC}
    accuracy: ${accuracy}
    pValue: ${pValue}
  """
}
trait HasBinaryMetrics {
    val metrics:BinaryMetrics
}

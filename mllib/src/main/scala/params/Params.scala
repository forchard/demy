package demy.mllib.params

import org.apache.spark.sql.Dataset
import org.apache.spark.ml.param.{Param, ParamValidators, Params}
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors
import org.apache.spark.ml.param.shared

trait HasParallelismDemy extends Params {
  val parallelism = new Param[Int](this, "parallelism", "the number of threads to use when running parallel algorithms", ParamValidators.gtEq(1))
  setDefault(parallelism -> 1)
  def getParallelism: Int = get(parallelism).get
  def getExecutionContext: ExecutionContext = ExecutionContext.fromExecutor(Executors.newFixedThreadPool(getParallelism))
  def setParallelism(value: Int): this.type = set(parallelism, value)
}

trait HasScoreCol extends Params {
  val scoreCol = new Param[String](this, "scoreCol", "the column where score is stored")
  setDefault(scoreCol -> "score")
  def getScoreCol: String = get(scoreCol).get
  def setScoreCol(value: String): this.type = set(scoreCol, value)
}

trait HasExecutionMetrics extends Params {
  val logMetrics = new Param[Boolean](this, "logMetrics", "If this step should log execution metrics")
  val metricsToLog = new Param[Array[String]](this, "metricsToLog", "the metrics to log (all by default)")
  setDefault(logMetrics -> false, metricsToLog -> Array[String]())
  def setLogMetrics(value: Boolean): this.type = set(logMetrics, value)
  def setMetricsToLog(value:Array[String]): this.type = set(metricsToLog, value)
  def getLogMetrics: Boolean = getOrDefault(logMetrics)
  def getMetricsToLog: Array[String] = getOrDefault(metricsToLog)
  val metrics:scala.collection.mutable.Map[String, Double] = scala.collection.mutable.Map[String, Double]()
}

trait HasFolds extends Params {
  val numFolds = new Param[Int](this, "numFolds", "The number of random folds to build")
  def setNumFolds(value: Int): this.type = set(numFolds, value)
}

trait HasTrainRatio extends Params {
  val trainRatio = new Param[Double](this, "trainRatio", "The train set ratio as a proportion e.g. 0.75 meand that 3/4 of randomly selected rows will be used for training (not compatible folds > 1")
  def setTrainRatio(value: Double): this.type = set(trainRatio, value)
}

trait HasInputCols extends shared.HasInputCols {
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)
}

trait HasGroupByCols extends Params {
  val groupByCols = new Param[Array[String]](this, "groupByCols", "The columns to group by")
  def setGroupByCols(value: Array[String]): this.type = set(groupByCols, value)
}

trait HasStratifyByCols extends Params {
  val stratifyByCols = new Param[Array[String]](this, "stratifyByCols", "The columns to group by")
  def setStratifyByCols(value: Array[String]): this.type = set(stratifyByCols, value)
}

trait HasProbabilityCol extends shared.HasProbabilityCol {
  def setProbabilityCol(value: String): this.type = set(probabilityCol, value)
}

trait HasFeaturesCol extends shared.HasFeaturesCol {
  def setFeaturesCol(value: String): this.type = set(featuresCol, value)
}

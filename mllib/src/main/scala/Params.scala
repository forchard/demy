package demy.mllib

import org.apache.spark.sql.Dataset
import org.apache.spark.ml.param.{Param, ParamValidators, Params}
import scala.concurrent.ExecutionContext
import java.util.concurrent.Executors

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


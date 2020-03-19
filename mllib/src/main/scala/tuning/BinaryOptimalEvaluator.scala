package demy.mllib.tuning

import demy.mllib.params.HasScoreCol
import demy.mllib.evaluation.{HasBinaryMetrics, BinaryMetrics}
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.attribute.AttributeGroup
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col, lit}
import org.apache.spark.storage.StorageLevel
import demy.util.{log => l}

trait BinaryOptimalEvaluatorBase extends HasLabelCol with HasScoreCol with HasPredictionCol {
  val uid: String
  final val bins = new Param[Int](this, "bins", "The number of bins for thresholds")
  final val optimize = new Param[String](this, "optimize", "The optimization mode (f1Score, precision:0.85, recall:0.7, accuracy)")
  final val evaluationFilter = new Param[String](this, "evaluationFilter", "A sql forlula on input dataframe to limit the rows that are going to be evaluated")
  setDefault(bins->100, optimize->"f1score")
  def setLabelCol(value: String): this.type = set(labelCol, value)
  def setPredictionCol(value: String): this.type = set(predictionCol, value)
  def setEvaluationFilter(value: String): this.type = set(evaluationFilter, value)
  def setBins(value: Int): this.type = set(bins, value)
  def setOptimize(value: String): this.type = set(optimize, value)
  def validateAndTransformSchema(schema: StructType): StructType = StructType(schema.filterNot(f => f.name == getOrDefault(predictionCol)) :+ (new AttributeGroup(name=get(predictionCol).get).toStructField))
}
object BinaryOptimalEvaluatorBase {
}

class BinaryOptimalEvaluator(override val uid: String) extends Estimator[BinaryOptimalEvaluatorModel] with BinaryOptimalEvaluatorBase {
  override def fit(ds: Dataset[_]): BinaryOptimalEvaluatorModel = {
    val dataset = get(evaluationFilter) match {case Some(s) => ds.where(s) case _ => ds}
    import dataset.sparkSession.implicits._
    val labelType = dataset.select(getOrDefault(labelCol)).schema.fields(0).dataType
    val labelExp = (labelType match {
      case IntegerType =>  udf((label:Int)=> Vectors.dense(label.toDouble))
      case DoubleType =>  udf((label:Double)=> Vectors.dense(label))
      case FloatType =>  udf((label:Double)=> Vectors.dense(label))
      case _ => udf((label:MLVector) => label)
    }).apply(col(getOrDefault(labelCol)).as(getOrDefault(labelCol)))

    val scoreType = dataset.select(getOrDefault(scoreCol)).schema.fields(0).dataType
    val scoreExp = (scoreType match {
      case DoubleType =>  udf((score:Double)=> Vectors.dense(score))
      case FloatType =>  udf((score:Double)=> Vectors.dense(score))
      case _ => udf((score:MLVector) => score)
    }).apply(col(getOrDefault(scoreCol)).as(getOrDefault(scoreCol)))
    val toEvaluate = dataset.select(scoreExp, labelExp).as[(MLVector,MLVector)]
                            .flatMap(p => p match {case (scores, labels) => scores.toArray.zip(labels.toArray)})
                            .rdd.persist(StorageLevel.MEMORY_AND_DISK)

    val binMetrics = get(bins) match {
      case Some(f) => new BinaryClassificationMetrics(toEvaluate, f)
      case _ => new BinaryClassificationMetrics(toEvaluate)
    }

    val midThreshold =
      binMetrics
        .thresholds
        .reduce((t1, t2) => if(t1 < 0.5 && t2 < 0.5) {if(t1>t2) t1 else t2}
          else if (t1 < 0.5 && t2 >=0.5) t2
          else if (t1 >= 0.5 && t2 < 0.5) t1
          else /*t1>=0.5 && t2>=0.5*/ {if(t1<t2) t1 else t2}
        )

    val f1s = binMetrics.fMeasureByThreshold()
    val precs = binMetrics.precisionByThreshold()
    val recs = binMetrics.recallByThreshold()
    val optimizeAs = getOrDefault(optimize)
    val optimalThreshold =
      if(optimizeAs.startsWith("precision:")) {
        l.msg("optimizing precision")
        val precLimit = optimizeAs.replace("precision:", "").toDouble
        precs
          .zip(recs)
          .reduce((p1, p2) => (p1, p2) match {
            case (((thres1, prec1),(_, rec1)), ((thres2, prec2), (_, rec2))) =>
              if(prec1 >= precLimit && prec2 >= precLimit) {
                if(rec1 > rec2) ((thres1, prec1), (thres1, rec1))
                else ((thres2, prec2), (thres2, rec2))
              }
              else if(prec1 >= precLimit) ((thres1, prec1), (thres1, rec1))
              else if(prec2 >= precLimit) ((thres2, prec2), (thres2, rec2))
              else ((0.0, 0.0), (0.0, 0.0))
        })
        ._1._1
      }
      else if(optimizeAs.startsWith("recall:")) {
        l.msg("optimizing recall")
        val recLimit = optimizeAs.replace("recall:", "").toDouble
        precs
          .zip(recs)
          .reduce((p1, p2) => (p1, p2) match {
            case (((thres1, prec1),(_, rec1)), ((thres2, prec2), (_, rec2))) =>
             if(rec1 >= recLimit && rec2 >= recLimit) {if(prec1 > prec2) ((thres1, prec1), (thres1, rec1)) else ((thres2, prec2), (thres2, rec2)) }
             else if(rec1 >= recLimit) ((thres1, prec1), (thres1, rec1))
             else if(rec2 >= recLimit) ((thres2, prec2), (thres2, rec2))
             else ((0.0, 0.0), (0.0, 0.0))
          })
          ._1._1
      }
      else if(optimizeAs.startsWith("prec/recall:")) {
        l.msg("optimizing precRecall ratio")
        val ratioLimit = optimizeAs.replace("prec/recall:", "").toDouble
        precs
          .zip(recs)
          .reduce((p1, p2) => (p1, p2) match {
            case (((thres1, prec1),(_, rec1)), ((thres2, prec2), (_, rec2))) =>
             if(Math.abs(prec1/rec1-ratioLimit) <= Math.abs(prec2/rec2-ratioLimit)) ((thres1, prec1), (thres1, rec1))
             else ((thres2, prec2), (thres2, rec2))
           })
          ._1._1
      }
      else {
        l.msg("optimizing f1")
        f1s.reduce((p1, p2) => if(p1._2 > p2._2) p1 else p2)._1
      }

    val basePrecision = Some(
      precs
        .filter(p => p._1 == midThreshold)
        .map(p => p._2)
        .first
      )
    val precision =
      precs
        .filter(p => p._1 == optimalThreshold)
        .map(p => p._2)
        .take(1) match {
          case Array(v) => Some(v)
          case _ => None
        }
    val baseRecall = Some(
      recs
        .filter(p => p._1 == midThreshold)
        .map(p => p._2)
        .first
      )
    val recall =
      recs
        .filter(p => p._1 == optimalThreshold)
        .map(p => p._2)
        .take(1) match {
          case Array(v) => Some(v)
          case _ => None
        }
    val baseF1Score = Some(
      f1s
        .filter(p => p._1 == midThreshold)
        .map(p => p._2)
        .first
      )

    val f1Score =
      f1s
        .filter(p => p._1 == optimalThreshold)
        .map(p => p._2)
        .take(1) match {
          case Array(v) => Some(v)
          case _ => None
        }
    val areaUnderROC = Some(binMetrics.areaUnderROC())
    val rocCurve = binMetrics.roc().collect

    val (tp, tn, fp, fn) =
      toEvaluate
        .map(p => p match { case (score, label) =>
          ( if(label==1 && score >= optimalThreshold) 1 else 0
             ,if(label==0 && score < optimalThreshold) 1 else 0
             ,if(label==0 && score >= optimalThreshold) 1 else 0
             ,if(label==1 && score < optimalThreshold) 1 else 0
          )
        })
        .reduce((p1, p2) => (p1, p2) match {
          case ((tp1, tn1, fp1, fn1),(tp2, tn2, fp2, fn2)) =>
            (tp1 + tp2, tn1 + tn2, fp1 + fp2, fn1 + fn2)
        })

    println("tp: "+tp+", tn: "+tn+", fp: "+fp+", fn: "+fn)
    val accuracy = (tp.toDouble+tn.toDouble) / (tp.toDouble+tn.toDouble+fp.toDouble+fn.toDouble)
    val precT = tp.toDouble / (tp.toDouble+fp.toDouble)
    val recallT = tp.toDouble / (tp.toDouble+fn.toDouble)
    val f1scT = 2*precT*recallT/(precT+recallT)
    println("acc: "+accuracy+", prec: "+precision+", recall: "+recall+", f1score: "+f1Score)

    val df = dataset.sparkSession.createDataFrame(toEvaluate
                                    .map(p => p match { case (score, label) =>
                                      if (score >= optimalThreshold) (Vectors.dense(1.0), label) else (Vectors.dense(0.0), label)
                                  }))
                    .toDF("prediction","true")
//    println(df.show())
    val chi2 = ChiSquareTest.test(df, "prediction", "true").head
    val pValue = chi2.getAs[MLVector](0).toArray
    val degreesOfFreedom = chi2.getSeq[Int](1).mkString("[", ",", "]")
    val statistics = chi2.getAs[MLVector](2).toArray


    val metrics = BinaryMetrics(threshold=Some(optimalThreshold), tp = Some(tp), tn=Some(tn), fp=Some(fp), fn=Some(fn)
                          , basePrecision=basePrecision, precision=precision, baseRecall=baseRecall, recall=recall, baseF1Score=baseF1Score
                          , f1Score=f1Score, areaUnderROC=areaUnderROC, rocCurve=rocCurve
                          , accuracy = Some(accuracy), pValue = Some(pValue(0)))

    copyValues(new BinaryOptimalEvaluatorModel(uid, metrics).setParent(this))
  }
  override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
  def copy(extra: ParamMap): this.type = {defaultCopy(extra)}
  def this() = this(Identifiable.randomUID("BinaryEvaluator"))
};class BinaryOptimalEvaluatorModel(override val uid: String, val metrics:BinaryMetrics) extends org.apache.spark.ml.Model[BinaryOptimalEvaluatorModel] with BinaryOptimalEvaluatorBase with HasBinaryMetrics{
    override def transform(dataset: Dataset[_]): DataFrame = {
        dataset.withColumn(getOrDefault(predictionCol), udf((score:MLVector, threshold:Double)=>{
          Vectors.dense(score.toArray.map(s => if(s<threshold) 0.0 else 1.0))
        }).apply(col(getOrDefault(scoreCol)), lit(metrics.threshold.get)))
    }
    override def transformSchema(schema: StructType): StructType = schema
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}
    def this() = this(Identifiable.randomUID("BinaryOptimalEvaluatorModel"), null)
};object BinaryOptimalEvaluatorModel {
}

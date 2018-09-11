package demy.mllib.tuning

import demy.mllib.HasScoreCol
import demy.mllib.evaluation.{HasBinaryMetrics, BinaryMetrics}
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.attribute.AttributeGroup 
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col, lit}
import org.apache.spark.storage.StorageLevel

trait BinaryOptimalEvaluatorBase extends HasLabelCol with HasScoreCol with HasPredictionCol {
    val uid: String
    final val bins = new Param[Int](this, "folds", "The number of bins for thresholds")
    def setLabelCol(value: String): this.type = set(labelCol, value)
    def setPredictionCol(value: String): this.type = set(predictionCol, value)
    def setBins(value: Int): this.type = set(bins, value)

    def validateAndTransformSchema(schema: StructType): StructType = StructType(schema.filterNot(f => f.name == getOrDefault(predictionCol)) :+ (new AttributeGroup(name=get(predictionCol).get).toStructField))
}
object BinaryOptimalEvaluatorBase {
}

class BinaryOptimalEvaluator(override val uid: String) extends Estimator[BinaryOptimalEvaluatorModel] with BinaryOptimalEvaluatorBase {
    override def fit(dataset: Dataset[_]): BinaryOptimalEvaluatorModel = {
      import dataset.sparkSession.implicits._
      val toEvaluate = dataset.select(getOrDefault(scoreCol), getOrDefault(labelCol)).as[(MLVector,MLVector)].flatMap(p => p match {case (scores, labels) => scores.toArray.zip(labels.toArray)}).rdd.persist(StorageLevel.MEMORY_AND_DISK)
      val binMetrics = get(bins) match {case Some(f) => new BinaryClassificationMetrics(toEvaluate, f) case _ => new BinaryClassificationMetrics(toEvaluate)}
      val midThreshold = binMetrics.thresholds.filter(t => t>0.5).reduce((t1, t2)=> if(t1 < t2) t1 else t2)
      val f1s = binMetrics.fMeasureByThreshold()
      val precs = binMetrics.precisionByThreshold()
      val recs = binMetrics.recallByThreshold()

      val optimalThreshold = f1s.reduce((p1, p2) => if(p1._2 > p2._2) p1 else p2)._1
      val basePrecision = Some(precs.filter(p => p._1 == midThreshold).map(p => p._2).first)
      val precision = Some(precs.filter(p => p._1==optimalThreshold).map(p => p._2).first)
      val baseRecall = Some(recs.filter(p => p._1==midThreshold).map(p => p._2).first)
      val recall = Some(recs.filter(p => p._1==optimalThreshold).map(p => p._2).first)
      val baseF1Score = Some(f1s.filter(p => p._1==midThreshold).map(p => p._2).first )
      val f1Score = Some(f1s.filter(p => p._1==optimalThreshold).map(p => p._2).first)
      val areaUnderROC = Some(binMetrics.areaUnderROC())
      val rocCourve = binMetrics.roc().collect

      val (tp, tn, fp, fn) = toEvaluate.map(p => p match {case (score, label) => (if(label==1 && score >= optimalThreshold) 1 else 0
                                                                                  ,if(label==0 && score < optimalThreshold) 1 else 0
                                                                                  ,if(label==0 && score >= optimalThreshold) 1 else 0
                                                                                  ,if(label==1 && score < optimalThreshold) 1 else 0
                                                                                  )})
                                        .reduce((p1, p2) => (p1, p2) match {case ((tp1, tn1, fp1, fn1),(tp2, tn2, fp2, fn2)) => (tp1 + tp2, tn1 + tn2, fp1 + fp2, fn1 + fn2)})
      

      val metrics = BinaryMetrics(threshold=Some(optimalThreshold), tp = Some(tp), tn=Some(tn), fp=Some(fp), fn=Some(fn)
                            , basePrecision=basePrecision, precision=precision, baseRecall=baseRecall, recall=recall, baseF1Score=baseF1Score
                            , f1Score=f1Score, areaUnderROC=areaUnderROC, rocCourve=rocCourve)
    
      new BinaryOptimalEvaluatorModel(uid, metrics)
           .setLabelCol(get(labelCol).get)
           .setPredictionCol(get(predictionCol).get)
           .setScoreCol(get(scoreCol).get)
           .setParent(this)
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("BinaryEvaluator"))
};class BinaryOptimalEvaluatorModel(override val uid: String, val metrics:BinaryMetrics) extends org.apache.spark.ml.Model[BinaryOptimalEvaluatorModel] with BinaryOptimalEvaluatorBase with HasBinaryMetrics{
    override def transform(dataset: Dataset[_]): DataFrame = {
        dataset.withColumn(getOrDefault(predictionCol), udf((score:MLVector, threshold:Double)=>{
            Vectors.dense(score.toArray.map(s => if(s<=threshold) 0.0 else 1.0))
        }).apply(col(getOrDefault(scoreCol)), lit(metrics.threshold.get)))
    }
    override def transformSchema(schema: StructType): StructType = schema
    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("BinaryOptimalEvaluatorModel"), null)
};object BinaryOptimalEvaluatorModel {
}

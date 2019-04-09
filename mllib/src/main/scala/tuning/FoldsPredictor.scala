package demy.mllib.tuning

import demy.mllib.params.HasParallelismDemy
import demy.mllib.util.log.{msg, debug}
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.functions.{lit, expr, not}
import org.apache.spark.sql.types._
import org.apache.spark.ml.param.shared._
import scala.concurrent.{Await, Future, ExecutionContext, blocking}
import scala.concurrent.duration.Duration


abstract class Folder extends Params {
  def buildFolds[T](ds:Dataset[T]):Array[(Dataset[T], Dataset[T])]
}

trait FoldsPredictorBase extends HasParallelismDemy with HasPredictionCol with HasLabelCol with HasRawPredictionCol with HasFeaturesCol {
    val uid: String
    val estimator = new Param[FoldsPredictorBase.EstimatorType](this, "estimator", "the estimator that will build the model")
    val folder = new Param[FoldsPredictorBase.FolderType](this, "folder", "the folder that will define the folds to train and validate the model")
    val forceTestOn = new Param[String](this, "forceTestOn", "SQL formula for lines that are only going to be used to test")
    val forceTrainOn = new Param[String](this, "forceTrainOn", "SQL formula for lines that are only going to be used to training")
    def setFeaturesCol(value: String): this.type = set(featuresCol, value)
    def setLabelCol(value: String): this.type = set(labelCol, value)
    def setPredictionCol(value: String): this.type = set(predictionCol, value)
    def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
    def setEstimator(value: FoldsPredictorBase.EstimatorType): this.type = set(estimator, value)
    def setFolder(value: FoldsPredictorBase.FolderType): this.type = set(folder, value)
    def setForceTestOn(value: String): this.type = set(forceTestOn, value)
    def setForceTrainOn(value: String): this.type = set(forceTrainOn, value)
    def validateAndTransformSchema(schema: StructType): StructType = get(estimator).get.transformSchema(schema)
}

object FoldsPredictorBase {
    type FolderType = Folder
    type EstimatorType = Estimator[M] with HasPredictionCol with HasLabelCol with HasRawPredictionCol with HasFeaturesCol forSome {type M <: ModelType } 
    type ModelType = Transformer with HasPredictionCol with HasLabelCol with HasRawPredictionCol with HasFeaturesCol
}

class FoldsPredictor(override val uid: String) extends Estimator[FoldsPredictorModel] with FoldsPredictorBase {
    override def fit(dataset: Dataset[_]): FoldsPredictorModel = {
      var baseEstimator = get(estimator).get
      baseEstimator = baseEstimator
            .set(baseEstimator.predictionCol, get(predictionCol).get)
            .set(baseEstimator.labelCol, get(labelCol).get)
            .set(baseEstimator.featuresCol, get(featuresCol).get)
            .set(baseEstimator.rawPredictionCol, get(rawPredictionCol).get)

      val fullModel = baseEstimator.fit(dataset)
      
      new FoldsPredictorModel(uid, fullModel)
           .setFeaturesCol(get(featuresCol).get)
           .setLabelCol(get(labelCol).get)
           .setPredictionCol(get(predictionCol).get)
           .setRawPredictionCol(get(rawPredictionCol).get)
           .setEstimator(get(estimator).get)
           .setFolder(get(folder).get)
           .setParent(this)
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): FoldsPredictor = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("FoldsPredictor"))
}

class FoldsPredictorModel(override val uid: String, val model:FoldsPredictorBase.ModelType) extends org.apache.spark.ml.Model[FoldsPredictorModel] with FoldsPredictorBase {
    override def transform(ds: Dataset[_]): DataFrame = typedTransform(ds)
    def typedTransform[T](ds: Dataset[T]): DataFrame = {
      val folderTrans = get(folder).get
      val forcedTrainSet = get(forceTrainOn) match {case Some(trainExp) => Some(ds.where(expr(trainExp))) case _ => None} 
      val forcedTestSet = get(forceTestOn) match {case Some(testExp) => Some(ds.where(expr(testExp))) case _ => None}
      val toFoldDs = (get(forceTrainOn), get(forceTestOn)) match {
                       case (Some(trainExp), Some(testExp)) => ds.where(not(expr(trainExp)) && not(expr(testExp)))
                       case (Some(trainExp), None) => ds.where(not(expr(trainExp)))
                       case (None, Some(testExp)) => ds.where(not(expr(testExp)))
                       case (None, None) => ds
                     }
      val folds = folderTrans
            .buildFolds(toFoldDs)
      var baseEstimator = get(estimator).get
      baseEstimator = baseEstimator
            .set(baseEstimator.predictionCol, getOrDefault(predictionCol))
            .set(baseEstimator.labelCol, getOrDefault(labelCol))
            .set(baseEstimator.featuresCol, getOrDefault(featuresCol))
            .set(baseEstimator.rawPredictionCol, getOrDefault(rawPredictionCol))

      debug("training folds")
      val executionContext = getExecutionContext
      val predictionsFutures = folds.map(fold => fold match { case (trainFold, testFold) => {
        Future {
            blocking {
              val trainSet = forcedTrainSet match {case Some(forced) =>trainFold.union(forced) case _ => trainFold }
              debug(s"training on ${trainSet.count} and resting on ${testFold.count}")
              val model = baseEstimator.fit(trainSet)
              val preds = model.transform(testFold)
              preds
            }
        }(executionContext)
      }})
      val foldPredictions = predictionsFutures.map(future => Await.result(future, Duration.Inf)).reduce((ds1, ds2) => ds1.union(ds2))
      debug("folds trained")

      val predictions  = get(forceTestOn) match {
        case Some(forceTestExp) => 
          debug("training with all folds for forced test set")
          val trainSet = ds.where(not(expr(forceTestExp)))
          val testSet = ds.where(expr(forceTestExp))
          val model = baseEstimator.fit(trainSet)
          val forcedPredictions = model.transform(testSet)
          foldPredictions.union(forcedPredictions)
        case None =>
          foldPredictions
      }
      predictions
    }
    override def transformSchema(schema: StructType): StructType = schema
    def copy(extra: ParamMap): FoldsPredictorModel = {
          val copied = new FoldsPredictorModel(this.uid, this.model)
              copyValues(copied, extra).setParent(parent)
    }    
    def this() = this(Identifiable.randomUID("FoldsPredictorModel"), null)
}

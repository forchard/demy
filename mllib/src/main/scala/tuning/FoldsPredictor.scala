package demy.mllib.tuning

import demy.mllib.{HasParallelismDemy}
import demy.mllib.util.log.msg
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.ml.param.shared._


abstract class Folder extends Params {
  def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])]
}

trait FoldsPredictorBase extends HasParallelismDemy with HasPredictionCol with HasLabelCol with HasRawPredictionCol with HasFeaturesCol {
    val uid: String
    val estimator = new Param[FoldsPredictorBase.EstimatorType](this, "estimator", "the estimator that will build the model")
    val folder = new Param[FoldsPredictorBase.FolderType](this, "folder", "the folder that will define the folds to train and validate the model")
    def setFeaturesCol(value: String): this.type = set(featuresCol, value)
    def setLabelCol(value: String): this.type = set(labelCol, value)
    def setPredictionCol(value: String): this.type = set(predictionCol, value)
    def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
    def setEstimator(value: FoldsPredictorBase.EstimatorType): this.type = set(estimator, value)
    def setFolder(value: FoldsPredictorBase.FolderType): this.type = set(folder, value)
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
    override def transform(dataset: Dataset[_]): DataFrame = {
      val folderTrans = get(folder).get
      val folds = folderTrans
            .buildFolds(dataset)
      var baseEstimator = get(estimator).get
      baseEstimator = baseEstimator
            .set(baseEstimator.predictionCol, get(predictionCol).get)
            .set(baseEstimator.labelCol, get(labelCol).get)
            .set(baseEstimator.featuresCol, get(featuresCol).get)
            .set(baseEstimator.rawPredictionCol, get(rawPredictionCol).get)

      msg("training folds")
      val predictions = folds.map(fold => fold match { case (train, test) => {
          val model = baseEstimator.fit(train)
          val predictions = model.transform(test)
          predictions
      }}).reduce((ds1, ds2) => ds1.union(ds2))
      msg("folds trained")
      predictions
    }
    override def transformSchema(schema: StructType): StructType = schema
    def copy(extra: ParamMap): FoldsPredictorModel = {
          val copied = new FoldsPredictorModel(this.uid, this.model)
              copyValues(copied, extra).setParent(parent)
    }    
    def this() = this(Identifiable.randomUID("FoldsPredictorModel"), null)
}

package demy.mllib.text;

import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.classification.{Classifier, ClassificationModel}
class TermlLikelyhoodEvaluator {
  /**
   * Model Training:
   *   Vector Dictionary: vectors:DataFrame, WordColumnName:String, VectorColumnName:String
   *   EntitiesTrainingSetSize:Int
   *   NonEntitiesTrainingSetSize:Int
   *   classifier:Classifier
   *    ==>
   *   classificationModel:ClassificationModel
   * Applying Model
   *   TextsToQualify: Dataframe, TermColumName:String
   *   >> DataFrame ++ LikelyhoodColumn()
   */
}


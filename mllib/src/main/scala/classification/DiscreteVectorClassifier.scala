package demy.mllib.classification

import demy.mllib.HasParallelismDemy
import demy.mllib.util.log
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.ml.classification.{Classifier, ClassificationModel}
import org.apache.spark.ml.attribute.AttributeGroup 
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, lit, col}
import org.apache.spark.storage.StorageLevel
import scala.concurrent.{Await, Future, ExecutionContext, blocking}
import scala.concurrent.duration.Duration


trait DiscreteVectorBase extends HasParallelismDemy with HasPredictionCol with HasLabelCol with HasFeaturesCol with HasRawPredictionCol {
    val uid: String
//    val models:Array[DiscreteVectorBase.ModelType] 
    final val classifier = new Param[DiscreteVectorBase.ClassifierType](this, "classifier", "base binary classifier")
    def setFeaturesCol(value: String): this.type = set(featuresCol, value)
    def setLabelCol(value: String): this.type = set(labelCol, value)
    def setPredictionCol(value: String): this.type = set(predictionCol, value)
    def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
    def setClassifier(value: DiscreteVectorBase.ClassifierType): this.type = set(classifier, value)
    def validateAndTransformSchema(schema: StructType): StructType = {
        schema.add(new AttributeGroup(name=get(predictionCol).get).toStructField)
              .add(StructField(get(rawPredictionCol).get
                   , ArrayType(elementType=(new AttributeGroup(name="dummy")).toStructField.dataType, containsNull = true) ))
        
    }
};object DiscreteVectorBase {
    type ModelType = ClassificationModel[F, M] forSome {type F <: MLVector; type M <: ClassificationModel[F, M]}
    type ClassifierType = Classifier[F, E, M] forSome {type F <: MLVector; type M <: ClassificationModel[F, M]; type E <: Classifier[F, E, M]}
};class DiscreteVectorModel(override val uid: String, val models:Array[DiscreteVectorBase.ModelType]) extends org.apache.spark.ml.Model[DiscreteVectorModel] with DiscreteVectorBase {
    override def transform(dataset: Dataset[_]): DataFrame = {
        import dataset.sparkSession.implicits._
        val df = dataset.toDF
        val outSchema = validateAndTransformSchema(df.schema)
        val featuresColName = get(featuresCol).get
        val rdd = df.rdd.mapPartitions(iter => { 
          val rawPredicters = models.map{model => val method = model.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
                                         method.setAccessible(true)
                                         (model, method)
                                }
          
          iter.map{r => 
            val newValues = r.toSeq ++ ({
              val features = r.getAs[MLVector](featuresColName)
              if(features == null) throw new Exception("Please not null!!!!")
              val rawPredictions = rawPredicters.map(p => p match { case (model, method) => method.invoke(model, features).asInstanceOf[MLVector] })
              val predictions = Vectors.dense(rawPredictions.toArray.map(clases => {
                  var (i, bestIndex,  bestValue) = (0, 0, Double.MinValue)
                  while (i < clases.size) {
                    if(clases(i)>bestValue) {
                      bestIndex = i
                      bestValue = clases(i)
                    }
                    i = i + 1
                  }
                  bestIndex.toDouble
              }))
              Seq(predictions, rawPredictions)
            })
            Row.fromSeq(newValues)
          }
        })

        dataset.sparkSession.createDataFrame(rdd, outSchema)
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): DiscreteVectorModel = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("DiscreteVectorModel"), Array[DiscreteVectorBase.ModelType]())
};class DiscreteVectorClassifier(override val uid: String) extends Estimator[DiscreteVectorModel] with DiscreteVectorBase {
    override def fit(dataset: Dataset[_]): DiscreteVectorModel = {
      val baseDF = dataset.select(get(featuresCol).get, get(labelCol).get)
      val handlePersistence = dataset.storageLevel == StorageLevel.NONE
      if (handlePersistence) {
        baseDF.persist(StorageLevel.MEMORY_AND_DISK)
      }
      val executionContext = getExecutionContext
      val modelFutures = Range(0, baseDF.first.getAs[MLVector](get(labelCol).get).size).map{index => 
        // generate new label metadata for the binary problem.
        //val newLabelMeta = BinaryAttribute.defaultAttr.withName("label").toMetadata()
        val labelColName = "dvc$" + index
        val trainingDataset = baseDF.withColumn(labelColName, udf((vector:MLVector, index:Int)=>vector.apply(index)).apply(col(get(labelCol).get), lit(index))/*, newLabelMeta*/)
        val theClassifier = get(classifier).get
        val paramMap = new ParamMap()
        paramMap.put(theClassifier.labelCol -> labelColName)
        paramMap.put(theClassifier.featuresCol -> get(featuresCol).get)
        paramMap.put(theClassifier.predictionCol -> get(predictionCol).get)

        Future {
            blocking {
              demy.mllib.util.log.msg(s"Fitting $index !!!! having ${trainingDataset.count} lines **********************") 
              theClassifier.fit(trainingDataset, paramMap)
           }
        }(executionContext)
      }
      val models = modelFutures.map(future => Await.result(future, Duration.Inf)).toArray[ClassificationModel[F, M] forSome {type F <: MLVector; type M <: ClassificationModel[F, M]}]

      if (handlePersistence) {
        baseDF.unpersist()
      }
      new DiscreteVectorModel(uid, models)
           .setFeaturesCol(get(featuresCol).get)
           .setLabelCol(get(labelCol).get)
           .setPredictionCol(get(predictionCol).get)
           .setClassifier(get(classifier).get)
           .setRawPredictionCol(get(rawPredictionCol).get)
           .setParallelism(get(parallelism).get)
           .setParent(this)
    }
    override def transformSchema(schema: StructType): StructType = validateAndTransformSchema(schema)
    def copy(extra: ParamMap): DiscreteVectorClassifier = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("toStructField"))
}

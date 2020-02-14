package demy.mllib.topic

import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import java.lang.reflect.Method
import scala.{Iterator => It}

case class WrappedClassifier(model:LinearSVCModel) {
  val rawPredictMethod = {
    val m = model.getClass.getDeclaredMethod("predictRaw", classOf[MLVector])
    m.setAccessible(true)
    m
  }
  val predictMethod = {
    val m = model.getClass.getDeclaredMethod("predict", classOf[MLVector])
    m.setAccessible(true)
    m
  }

  def score(vector:MLVector) = {
    val scores = this.rawPredictMethod.invoke(this.model, vector).asInstanceOf[MLVector]
    val (yes, no) = (scores(1), scores(0))

    if(no>=0 && yes>=0) yes/(no + yes)
    else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
    else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
    else no/(no + yes)
  }
  def predict(vector:MLVector) = {
    this.predictMethod.invoke(this.model, vector).asInstanceOf[Double]
  }
  def getMetrics() = {

  }
}

object WrappedClassifier {
  def apply(forClass:Int, points:Seq[MLVector], pClasses:Seq[Int], spark:SparkSession):WrappedClassifier = {
    import spark.implicits._
    var (count0, count1) = (0, 0)
    val training =
      (for(i <- It.range(0, points.size))
        yield {
          (points(i)
            , if(pClasses(i)==forClass) {
                count1 = count1 + 1
                1.0
              } else {
                count0 = count0 + 1
                0.0
              }
      )})
        .toSeq
        .toDS
        .toDF("features", "label")
    if(count0 == 0 || count1 == 0) throw new Exception(s"Epi Wrapped Classifier needs two classes? Cannot train for class $forClass")

    val model = new LinearSVC().setRegParam(0.01).fit(training)
    WrappedClassifier(model = model)
  }
}

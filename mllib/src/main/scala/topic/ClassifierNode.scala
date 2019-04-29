package demy.mllib.topic

import demy.util.{log => l}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.lang.reflect.Method

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

case class ClassifierNode (
  name:String
  , tokens:ArrayBuffer[String] = ArrayBuffer[String]()
  , points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , pClasses:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  val algo = ClassAlgorithm.supervised
  val models = HashMap[Int, WrappedClassifier]()

  def encodeExtras(encoder:EncodedNode) {
    encoder.serialized += (("models", serialize(models.map{case(c, wrapped) => (c, wrapped.model)}))) 
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer
  }
  
  def transform(vClasses:Array[Int], scores:Option[Array[Double]], dag:Option[Array[Int]]
    , vectors:Seq[MLVector], tokens:Seq[String], spark:SparkSession) { 
      for{i <- It.range(0, vectors.size)
        if vectors(i) != null
        if this.params.inClasses.contains(vClasses(i)) } {
      val (bestClass, bestScore) = (
        (for(c <- this.params.outClasses) 
          yield(c, this.score(c, vectors(i))))
          .reduce((p1, p2) => (p1, p2) match {
            case ((c1, s1), (c2, s2)) => 
              if(s1 > s1) p1 else p2
          })
      )
      if(bestScore > 0.5) vClasses(i) = bestClass
      scores match {
        case Some(arr) => arr(i) = bestScore
        case None =>
      }
    }
  }

  def score(forClass:Int, vector:MLVector) = {
    this.models(forClass).score(vector)
  }

  def fit(spark:SparkSession) ={
    l.msg("start  classifier fitting models")
    this.models.clear
    for(c <- this.params.outClasses) {
      this.models(c) = WrappedClassifier(forClass = c, points = this.points, pClasses = this.pClasses, spark = spark) 
    }
    l.msg("clasifier fit")
    this
  }
}
object ClassifierNode {
  def apply(encoded:EncodedNode):ClassifierNode = {
    val ret = ClassifierNode(name = encoded.name, tokens = encoded.tokens.clone, points = encoded.points.clone, pClasses = encoded.pClasses.clone
      , params = encoded.params
    )
    ret.stepHits = encoded.stepHits
    ret.hits = encoded.hits
    ret.models ++= encoded.deserialize[HashMap[Int, LinearSVCModel]]("models").map{case(c, model) => (c, WrappedClassifier(model))}
    ret 
  }
}


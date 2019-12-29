package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.mllib.linalg.implicits._
import demy.util.{log => l}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.{Iterator => It}
import java.sql.Timestamp 


case class ClassifierNode (
  points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  val models = HashMap[Int, WrappedClassifier]()

  def encodeExtras(encoder:EncodedNode) {
    encoder.serialized += (("models", serialize(models.map(p => (p._1, p._2.model))))) 
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer
  }
  def toTag(id:Int):TagSource = ClassifierTagSource(
    id = id
    , operation = TagOperation.create 
    , timestamp = new Timestamp(System.currentTimeMillis())
    , name = this.params.name
    , inTag = this.params.strLinks.keys.map(_.toInt).toSet.toSeq match {case Seq(inTag) => inTag case _ => throw new Exception("Cannot transformle multi in classifier to Tag")}
    , outTags = this.params.strLinks.values.flatMap(e => e).toSet
    , oFilterMode = Some(this.params.filterMode)
    , oFilterValue = Some(this.params.filterValue.toSet)
    , vectorSize = this.params.vectorSize.get
  )
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:HashMap[Int, Double]
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGeneratror:Iterator[Int] 
      , fit:Boolean) { 
    for{(inClass, outClass) <- this.linkPairs
      (iIn, _) <- facts(inClass).iterator } {
        this.score(outClass, vectors(iIn)) match {
          case score => 
            if(score > 0.5) {
              facts.get(outClass) match {
                case Some(f) => f(iIn) = iIn
                case None => facts(outClass) = HashMap(iIn -> iIn)
              }
              scores.get(outClass) match {
                case Some(s) => scores(outClass) = if(s > score) s else score
                case None => scores(outClass) = score
              }
            }
        }
    }
  }

  def score(forClass:Int, vector:MLVector) = {
    this.models(forClass).score(vector)
  }

  def fit(spark:SparkSession) = {
    l.msg("start  classifier fitting models")
    this.models.clear
    for(c <- this.outClasses) {
      this.models(c) = 
        WrappedClassifier(
          forClass = c
          , points = this.points.filter(_ != null)
          , pClasses = 
            (for(i<-It.range(0, this.points.size)) 
              yield(this.rel(c).get(i) match {
                case Some(from) if this.inRel(c)((i, from)) => c
                case _ => -1
              })
            ).toSeq
             .zip(this.points)
             .flatMap{case(c, p) => if(p == null) None else Some(c)}
          , spark = spark) 
    }
    l.msg("clasifier fit")
    this
  }
  def mergeWith(that:Node, cGenerator:Iterator[Int], fit:Boolean):this.type = {
    this.params.hits = this.params.hits + that.params.hits
    It.range(0, this.children.size).foreach(i => this.children(i).mergeWith(that.children(i), cGenerator, fit))
    this
  }

  def updateParamsExtras {} 
  def resetHitsExtras {}
  def cloneUnfittedExtras = this
}
object ClassifierNode {
  def apply(params:NodeParams, index:Option[VectorIndex]):ClassifierNode = {
    val ret = ClassifierNode(
      points = ArrayBuffer[MLVector]() 
      , params = params
    )
    index match {
      case Some(ix) => ret.points ++= (ix(ret.sequences.flatMap(t => t).distinct) match {case map => ret.sequences.map(tts => tts.flatMap(token => map.get(token)).reduceOption(_.sum(_)).getOrElse(null))})
      case _ =>
    }
    ret
  }
  def apply(encoded:EncodedNode):ClassifierNode = {
    val ret = ClassifierNode(
      points = encoded.points.clone
      , params = encoded.params
    )
    ret.models ++= encoded.deserialize[HashMap[Int, LinearSVCModel]]("models").mapValues(m => WrappedClassifier(m))
    ret 
  }
}


package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}

case class AnalogyNode (
  points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  val models = HashMap[Int, WrappedClassifier]()
  val analogies = HashMap[Int, MLVector]()
  val referenceClass = params.names("referenceClass")
  val baseClass = params.names("baseClass")
  val analogyClass = params.names("analogyClass")
  assert(this.links.contains(baseClass))
  assert(this.links.contains(referenceClass))
  assert(this.links(baseClass)(analogyClass))

  def encodeExtras(encoder:EncodedNode) {
    encoder.serialized += (("models", serialize(models.map(p => (p._1, p._2.model))))) 
    encoder.serialized += (("analogies", serialize(analogies))) 
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer
  }
  def transform(facts:HashMap[Int, HashMap[Int, Int]], scores:Option[HashMap[Int, HashMap[Int, Double]]]=None, vectors:Seq[MLVector], tokens:Seq[String], cGeneratror:Iterator[Int]) { 
    (for((iRef, _) <- facts(referenceClass).iterator )
      yield (iRef, (
        for((iBase, _) <- facts(baseClass).iterator)
          yield 
          (iBase, this.analogyScore(forClass = analogyClass, reference = vectors(iRef), analogy = vectors(iBase)))
        ).reduceOption((p1, p2) => (p1, p2) match {case ((i1, s1), (i2, s2)) => if(s1 > s2) p1 else p2})
         .getOrElse((0, 0.0))
       ) 
    )
    .filter{case (iRef, (iBase, score)) => score > 0.5}
    .foreach{case (iRef, (iBase, score)) =>
      facts.get(analogyClass) match {
        case Some(f) => f(iBase) = iRef
        case None => facts(analogyClass) = HashMap(iBase -> iRef)
      }
      scores match {
        case Some(theScores) => {
          theScores.get(analogyClass) match {
            case Some(s) => s(iBase) = score
            case None => theScores(analogyClass) = HashMap(iBase -> score)
          }
        }
        case None =>
      }
    }
  }
  def canFitClassifier = this.inRel(analogyClass).values.toSet.size == 2 
  def fit(spark:SparkSession) ={
    l.msg("start fitting analogy models")
    this.models.clear
    this.analogies.clear
    val c = analogyClass
    if(this.canFitClassifier) {
      this.models(c) = 
        WrappedClassifier(
          forClass = c
          , points = 
            this.rel(c).iterator
              .map{case(iAna, iRef) => this.points(iRef).minus(this.points(iAna)) }
              .toSeq
          , pClasses = 
            this.rel(c).iterator
              .map{case (iAna, iRef) => if(this.inRel(c)((iAna, iRef))) c else -1}
              .toSeq
          , spark = spark
        )
    } else {
      this.analogies(c) = 
        (
          this.rel(analogyClass).iterator
              .map{case(iAna, iRef) => this.points(iRef).minus(this.points(iAna)) } 
        ) match {
          case analogies => analogies.reduce(_.sum(_)).scale(1.0/analogies.size)
        }
    }
    l.msg("analogy fit")
    this 
  }

  def analogyScore(forClass:Int, reference:MLVector, analogy:MLVector):Double = {
    this.models.get(forClass) match {
      case Some(classi) => classi.score(analogy.minus(reference))
      case None => this.analogies(forClass).sum(analogy).cosineSimilarity(reference) match {
        case v if(v<0) => 0
        case v => v
      }
    }
  }
  def mergeWith(that:Node):this.type = {
    this.params.hits = this.params.hits + that.params.hits
    It.range(0, this.children.size).foreach(i => this.children(i).mergeWith(that.children(i)))
    this
  }
  def resetHitsExtras {}
}

object AnalogyNode {
  def apply(params:NodeParams, index:VectorIndex):AnalogyNode = {
    val ret = AnalogyNode(
      points = ArrayBuffer[MLVector]() 
      , params = params
    )
    ret.points ++= (index(ret.tokens) match {case map => ret.tokens.map(t => map(t))})  
    ret
  }
  def apply(encoded:EncodedNode):AnalogyNode = {
    val ret = AnalogyNode(
      points = encoded.points.clone
      , params = encoded.params
    )
    ret.models ++= encoded.deserialize[HashMap[Int, LinearSVCModel]]("models").mapValues(m => WrappedClassifier(m))
    ret.analogies ++= encoded.deserialize[HashMap[Int, MLVector]]("analogies")
    ret 
  }
}


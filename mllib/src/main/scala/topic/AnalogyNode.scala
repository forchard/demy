package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.sql.Timestamp

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
  def toTag(id:Int):TagSource = AnalogyTagSource(
    id = this.params.tagId.getOrElse(id)
    , operation = TagOperation.create
    , timestamp = Some(new Timestamp(System.currentTimeMillis()))
    , name = Some(this.params.name)
    , color = this.params.color
    , referenceTag = Some(this.referenceClass.toInt)
    , baseTag = Some(this.referenceClass.toInt)
    , analogyClass = Some(this.analogyClass.toInt)
    , oFilterMode = Some(this.params.filterMode)
    , oFilterValue = Some(this.params.filterValue.toSet)
    )
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:HashMap[Int, Double]
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGeneratror:Iterator[Int]
      , fit:Boolean) {
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
      scores.get(analogyClass) match {
        case Some(s) => scores(analogyClass) = if(s > score) s else score
        case None => scores(analogyClass) = score
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
  def mergeWith(that:Node, cGenerator:Iterator[Int], fit:Boolean):this.type = {
    this.params.hits = this.params.hits + that.params.hits
//    TODO: merge externalClassesFreq for this that

    It.range(0, this.children.size).foreach(i => this.children(i).mergeWith(that.children(i), cGenerator, fit))
    this
  }
  def updateParamsExtras {}
  def resetHitsExtras {}
  def cloneUnfittedExtras = this
}

object AnalogyNode {
  def apply(params:NodeParams, index:Option[VectorIndex]):AnalogyNode = {
    val ret = AnalogyNode(
      points = ArrayBuffer[MLVector]()
      , params = params
    )
    index match {
      case Some(ix) => ret.points ++= (ix(ret.sequences.flatMap(t => t).distinct) match {case map => ret.sequences.map(tts => tts.flatMap(token => map.get(token)).reduceOption(_.sum(_)).getOrElse(null))})
      case _ =>
    }
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

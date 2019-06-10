package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.{Iterator => It}


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
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:Option[HashMap[Int, HashMap[Int, Double]]]=None
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGeneratror:Iterator[Int]) { 
    for{(inClass, outClass) <- this.linkPairs
      (iIn, _) <- facts(inClass).iterator } {
        this.score(outClass, vectors(iIn)) match {
          case score => 
            if(score > 0.5) {
              facts.get(outClass) match {
                case Some(f) => f(iIn) = iIn
                case None => facts(outClass) = HashMap(iIn -> iIn)
              }
              scores match {
                case Some(theScores) => {
                  theScores.get(outClass) match {
                    case Some(s) => s(iIn) = score
                    case None => theScores(outClass) = HashMap(iIn -> score)
                  }
                }
                case None =>
              }
            }
        }
    }
  }

  def score(forClass:Int, vector:MLVector) = {
    this.models(forClass).score(vector)
  }

  def fit(spark:SparkSession) ={
    l.msg("start  classifier fitting models")
    this.models.clear
    for(c <- this.outClasses) {
      this.models(c) = 
        WrappedClassifier(
          forClass = c
          , points = this.points
          , pClasses = 
            (for(i<-It.range(0, this.points.size)) 
              yield(this.rel(c).get(i) match {
                case Some(from) if this.inRel(c)((i, from)) => c
                case _ => -1
              })
            ).toSeq
          , spark = spark) 
    }
    l.msg("clasifier fit")
    this
  }
  def mergeWith(that:Node, cGenerator:Iterator[Int]):this.type = {
    this.params.hits = this.params.hits + that.params.hits
    It.range(0, this.children.size).foreach(i => this.children(i).mergeWith(that.children(i), cGenerator))
    this
  }

  def updateParamsExtras {} 
  def resetHitsExtras {}
  def cloneUnfittedExtras = this
}
object ClassifierNode {
  def apply(params:NodeParams, index:VectorIndex):ClassifierNode = {
    val ret = ClassifierNode(
      points = ArrayBuffer[MLVector]() 
      , params = params
    )
    ret.points ++= (index(ret.tokens) match {case map => ret.tokens.map(t => map(t))}) 
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


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
    id = this.params.tagId.getOrElse(id)
    , operation = TagOperation.create 
    , timestamp = Some(new Timestamp(System.currentTimeMillis()))
    , name = Some(this.params.name)
    , color = this.params.color
    , inTag = Some(this.params.strLinks.keys.map(_.toInt).toSet.toSeq match {case Seq(inTag) => inTag case _ => throw new Exception("Cannot transforme multi in classifier to Tag")})
    , outTags = Some(this.params.strLinks.values.flatMap(e => e).toSet)
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
    
    var setScores = (idxs:Iterator[Int], oClass:Int, score:Double ) => {
      var first:Option[Int] = None
      for(i <- idxs) {
        first = first.orElse(Some(i))
        facts.get(oClass) match {
          case Some(f) => f(i) = first.get
          case None => facts(oClass) = HashMap(i -> first.get)
        }
      }
      scores.get(oClass) match {
        case Some(s) => scores(oClass) = if(s > score) s else score
        case None => scores(oClass) = score
      }
    }    
    for((inClass, outClass) <- this.linkPairs) {
      val idx = facts(inClass).iterator.map{case (iIn, _) => iIn}.toSeq.sortWith(_ < _) 
      var allVector:Option[MLVector] = None
      var bestDocScore = 0.0
      var bestITo = -1
      var bestIndScore = 0.0
      var bestPosScore = 0.0
      var bestFrom = -1
      var bestTo = -1
      var sum:Option[MLVector] = None
      var bestSum:Option[MLVector] = None
 
      for{(iIn, iPos) <- idx.iterator.zipWithIndex} {
        Some(this.score(outClass, vectors(iIn)))
          .map{score => 
            if(score > 0.5) setScores(It(iIn), outClass, score)
            if(score > bestIndScore) bestIndScore = score
          } //always setting if current vector is classifies in the outClass

        allVector = Some(allVector.map(v => v.sum(vectors(iIn))).getOrElse(vectors(iIn)))
        if(iIn > bestITo) {//start expanding the right side right window
          bestIndScore = 0.0
          bestPosScore = 0.0
          bestFrom = iPos
          bestTo = iPos
          sum = None
          bestSum = None
          It.range(iPos, idx.size)
            .map(i => {
              sum = sum.map(v =>v.sum(vectors(idx(i)))).orElse(Some(vectors(idx(i))));
              (sum.get, i)
              })
            .map{case (vSum, i) => (i, this.score(outClass, vSum), vSum)}
            .map{case (i, score, vSum) => 
              if(score > bestPosScore) {
                bestPosScore = score
                bestTo = i
                bestITo = idx(i)
                bestSum = Some(vSum)
              }
              (i, score)
            }.takeWhile{case (i, score) => i < iPos + 2 || i < bestTo + 2}
            .size
            sum = bestSum
        } else { //contracting the left side window
          sum = sum.map(v => v.minus(vectors(idx(iPos -1))))
          sum
            .map{v => this.score(outClass, v)}
            .map{score => 
              if(score > bestPosScore) {
                bestPosScore = score
                bestFrom = iPos
              }
            }
        }
        if(iIn == bestITo) {
          if(bestPosScore > 0.5 && bestPosScore > bestIndScore) {
            setScores(It.range(bestFrom, bestTo + 1).map(i => idx(i)), outClass, bestPosScore)
          }
          if(bestPosScore > bestDocScore) bestDocScore = bestPosScore
          if(bestIndScore > bestDocScore) bestDocScore = bestIndScore
        }
      }
      allVector
        .map(v => this.score(outClass, v))
        .map{allScore => 
          if(allScore > bestDocScore && allScore > 0.5) {
            setScores(idx.iterator, outClass, bestDocScore)
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


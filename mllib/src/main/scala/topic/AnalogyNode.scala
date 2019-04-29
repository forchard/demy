package demy.mllib.topic

import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}

case class AnalogyNode (
  name : String
  , tokens:ArrayBuffer[String] = ArrayBuffer[String]()
  , points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , pClasses:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , pDag:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , pInAnalogy:ArrayBuffer[Boolean] = ArrayBuffer[Boolean]()
  , referenceClass:Int
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  val algo = ClassAlgorithm.analogy
  val models = HashMap[Int, WrappedClassifier]()
  val analogies = HashMap[Int, MLVector]()

  def encodeExtras(encoder:EncodedNode) {
    encoder.inAnalogy = pInAnalogy
    encoder.referenceClass = referenceClass 
    encoder.serialized += (("models", serialize(models.map{case(c, wrapped) => (c, wrapped.model)}))) 
    encoder.serialized += (("pDag", serialize(pDag))) 
    encoder.serialized += (("analogies", serialize(analogies))) 
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer
  }
  def transform(vClasses:Array[Int], scores:Option[Array[Double]], dag:Option[Array[Int]]
    , vectors:Seq[MLVector], tokens:Seq[String], spark:SparkSession) { 

    (for{iRef<-It.range(0, vectors.size) 
         if vectors(iRef) != null
         if referenceClass == vClasses(iRef) } 
      yield (iRef, (
        for{iAna <- It.range(0, vectors.size)
           if iRef != iAna
           if vectors(iAna) != null
           if this.params.inClasses.contains(vClasses(iAna)) } 
          yield 
          (iAna,
            (for(c <- this.params.outClasses.iterator) 
              yield 
                (c, this.analogyScore(forClass = c, reference = vectors(iRef), analogy = vectors(iAna)))
            ).reduce((p1, p2) => (p1, p2) match {case ((c1, s1), (c2, s2)) => if(s1 > s2) p1 else p2})
          )
        ).reduceOption((p1, p2) => (p1, p2) match {case ((i1, (c1, s1)), (i2, (c2, s2))) => if(s1 > s2) p1 else p2})
         .getOrElse((0, (0, 0.0)))
       ) 
    )
    .filter{case (iRef, (iAna, (anaClass, score))) => score > 0.5}
    .foreach{case (iRef, (iAna, (anaClass, score))) =>
      vClasses(iAna) = anaClass
      scores match {
        case Some(arr) => arr(iAna) = score
        case None =>
      }
      dag match {
        case Some(arr) => arr(iAna) = iRef
        case None =>
      }
    }
  }
  def canFitClassifier(c:Int) = pClasses.zip(pInAnalogy).filter(_._1 == c).map(_._2).distinct.size == 2 
  def fit(spark:SparkSession) ={
    l.msg("start fitting analogy models")
    this.models.clear
    this.analogies.clear
    for(c <- this.params.outClasses) {
       if(this.canFitClassifier(c)) {
         this.models(c) = 
           WrappedClassifier(
             forClass = c
             , points = 
               this.points.zip(this.pClasses).zip(this.pDag)
                 .filter{case((p, pc), r) => pc == c } 
                 .map{case((p, pc), r) => this.points(r).minus(p) } 
             , pClasses = 
               this.points.zip(this.pClasses).zip(this.pInAnalogy)
                 .filter{case((p, pc), ia) => pc == c } 
                 .map{case((p, pc), ia) => if(ia) c else 0 } 
                 .toSeq
             , spark = spark
           )
       } else {
         this.analogies(c) = 
           (this.points.zip(this.pClasses).zip(this.pDag)
               .filter{case((p, pc), r) => pc == c } 
               .map{case((p, pc), r) => this.points(r).minus(p) }
           ) match {
             case analogies => analogies.reduce(_.sum(_)).scale(1.0/analogies.size)
           }
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

  /*def addFeedback(forClass:Int, inputWord:String, outWord:String, inClass:Boolean, vectorIx:VectorIndex) = {
    this.points += vectorIx.getVector(inputWord).minus(vectorIx.getVector(outWord))
    this.pClasses += (if(inClass) forClass else 0)
  }*/
}

object AnalogyNode {
  def apply(encoded:EncodedNode):AnalogyNode = {
    val ret = AnalogyNode(name = encoded.name, tokens = encoded.tokens.clone, points = encoded.points.clone, pClasses = encoded.pClasses.clone
      , params = encoded.params
      , pDag = encoded.deserialize[ArrayBuffer[Int]]("pDag")
      , pInAnalogy = encoded.inAnalogy
      , referenceClass = encoded.referenceClass
    )
    ret.hits = encoded.hits
    ret.stepHits = encoded.stepHits
    ret.models ++= encoded.deserialize[HashMap[Int, LinearSVCModel]]("models").map{case(c, model) => (c, WrappedClassifier(model))}
    ret.analogies ++= encoded.deserialize[HashMap[Int, MLVector]]("analogies")
    ret 
  }
}


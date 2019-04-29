package demy.mllib.topic

import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}

case class ClusteringNode (
  name:String
  , tokens:ArrayBuffer[String] = ArrayBuffer[String]()
  , points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , pClasses:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
  , maxTopWords:Int
  , classCenters:Map[Int, Int]
  , vectorSize:Int
) extends Node {
  val algo = ClassAlgorithm.clustering
  var initializing = true
  val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  val pCenters = Array.fill(this.classCenters.values.toSet.size)(vZero)
  val vCenters = Array.fill(this.classCenters.values.toSet.size)(vZero)
  val cGAP = Array.fill(this.classCenters.values.toSet.size)(1.0)
  val exceptPCenters = Array.fill(maxTopWords)(vZero)
  val pScores = Array.fill(maxTopWords)(0.0)
  val cHits = Array.fill(this.classCenters.values.toSet.size)(0)
  
  def encodeExtras(encoder:EncodedNode) {
    encoder.classCenters = classCenters
    encoder.serialized += (("maxTopWords", serialize(maxTopWords)))
    encoder.serialized += (("vectorSize",serialize(vectorSize)))
    encoder.serialized += (("initializing", serialize(initializing)))
    encoder.serialized += (("pCenters",serialize(pCenters )))
    encoder.serialized += (("vCenters",serialize(vCenters )))
    encoder.serialized += (("cGAP",serialize(cGAP )))
    encoder.serialized += (("exceptPCenters", serialize(exceptPCenters)))
    encoder.serialized += (("pScores",serialize(pScores) ))
    encoder.serialized += (("cHits",serialize(cHits) ))
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> classCenters: ${classCenters}\n")
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> pCenters: ${pCenters.size}\n")
    buffer
  }
  
  def transform(vClasses:Array[Int], scores:Option[Array[Double]], dag:Option[Array[Int]]
    , vectors:Seq[MLVector], tokens:Seq[String], spark:SparkSession) { 
      for{i <- It.range(0, vectors.size)
        if vectors(i) != null
        if this.params.inClasses.contains(vClasses(i)) } {
      val (bestClass, bestScore) = this.score(vectors(i), tokens(i), vClasses(i))
      if(bestScore > 0.5) vClasses(i) = bestClass
      scores match {
        case Some(arr) => arr(i) = bestScore
        case None =>
      }
    }
  }

  def onInit(vector:MLVector, token:String, inClass:Int) = {
    if(initializing) {
      //println(s"checking... $inClass, ${this.params.links(inClass)} diff  ${this.pClasses.toSet}")
      val asClass = 
        this.params.links(inClass).diff(this.pClasses.toSet).headOption match {
          case Some(classToFill) if 
            It.range(0, this.points.size)
              .filter(i => this.params.links(inClass)(this.pClasses(i)))
              .filter(i => this.points(i).similarityScore(vector) > 0.999)
              .size == 0 
           =>
            this.tokens += token
            this.points += vector
            this.pClasses += classToFill
            //println(s"init pClasses: ${this.pClasses.toSeq}")
            classToFill
          case Some(classToFill) =>
            It.range(0, this.points.size)
              .filter(i => this.params.links(inClass)(this.pClasses(i)))
              .map(i => this.pClasses(i))
              .next
          case _ =>
            -1
      }
      initializing = this.pClasses.toSet != this.params.links.flatMap{case(from, to) => to}.toSet
      updatePointsStats
      asClass
    }
    else
      -1
  }
  
  def score(vector:MLVector, token:String, inClass:Int) = {
    onInit(vector, token, inClass) match {
      case init if init < 0 =>
        val (vClass, iCenter, cSimilarity) =  
         this.params.links(inClass)
           .map(outClass => (outClass, this.classCenters(outClass), vector.similarityScore(this.pCenters(this.classCenters(outClass)))))
           .reduce((t1, t2) => (t1, t2) match {
             case ((class1, iCenter1, score1), (class2, iCenter2, score2)) => 
               if(score1 > score2 && !score1.isNaN || score2.isNaN) t1 
               else t2
           })
         val (iPoint, pSimilarity) = 
           (for{i <- It.range(0, this.points.size)
               if this.pClasses(i) == vClass } 
              yield (i, vector.similarityScore(this.points(i)))
           ).reduce((p1, p2) => (p1, p2) match {
               case ((i1, score1), (i2, score2)) => 
                 if(score1 > score2) p1 
                 else p2
           })
         this.fitPoint(vector = vector, token= token, vClass = vClass, vScore = pSimilarity, iPoint = iPoint, iCenter=iCenter)
         (vClass, pSimilarity)
      case init if init >= 0 =>
         (init, 1.0)
    }
  }

  def fitPoint(vector:MLVector, token:String, vClass:Int, vScore:Double, iPoint:Int, iCenter:Int) {
    this.pScores(iPoint) = this.pScores(iPoint) + vScore
    this.vCenters(iCenter) = this.vCenters(iCenter).scale(cHits(iCenter) + 1.0).sum(vector).scale(1.0/(cHits(iCenter) + 2))
    if(this.tokens(iPoint) != token && !this.initializing)
      tryAsPoint(vector = vector, token = token, vClass = vClass, iPoint = iPoint, iCenter = iCenter)
    this.cGAP(iCenter) = 1.0 - this.pCenters(iCenter).similarityScore(this.vCenters(iCenter))
    cHits(iCenter) = cHits(iCenter) + 1
  }

  def tryAsPoint(vector:MLVector, token:String, vClass:Int, iPoint:Int, iCenter:Int) {
    var viPoint = iPoint
    //Evaluating replacing the closest (and given) point
    val replacingCenter = this.exceptPCenters(viPoint).sum(vector)
    if((1.0 - replacingCenter.similarityScore(this.vCenters(iCenter))) - this.cGAP(iCenter) < 0.0001) {
      //println(s"$step gap: ${this.cGAP(iCenter)} replacing $token by ${this.tokens(viPoint)}")
      this.points(viPoint) = vector
      this.tokens(viPoint) = token
      updatePointsStats
    } else {
      if(this.points.size < this.maxTopWords) {
        //Evaluating adding as a new Center
        val newCenter = this.pCenters(iCenter).sum(vector)
        if((1.0 - newCenter.similarityScore(this.vCenters(iCenter))) - this.cGAP(iCenter) < 0.0001) {
          //println(s"$step gap: ${this.cGAP(iCenter)} adding $token")
          this.points += vector
          this.tokens += token
          this.pClasses += vClass
          //println(s"tryas pClasses: ${this.pClasses.toSeq} init?:{$this.initializing}")
          viPoint = this.points.size - 1
          updatePointsStats
        }
      }
    }
    /*//evaluating removing current vector is better
    val withoutCenter = this.exceptPCenters(viPoint)
    if(1.0 - withoutCenter.similarityScore(this.vCenters(iCenter)) - this.cGAP(iCenter) < 0.0001
        && this.pClasses.filter(c => this.classCenters(c) == iCenter).size > 1) {
      //println(s"$step gap: ${this.cGAP(iCenter)} removing ${this.tokens(viPoint)}")
      this.points.remove(viPoint)
      this.tokens.remove(viPoint)
      this.pClasses.remove(viPoint)
      updatePointsStats
    }*/
  }

  def updatePointsStats {
    for(i <- It.range(0, this.pCenters.size)) this.pCenters(i) = this.vZero
    
    //println(s"init pClasses: ${this.pClasses.toSeq} (${this.points.size})")
    for(i <- It.range(0, this.points.size)) {
      this.pCenters(this.classCenters(this.pClasses(i))) = this.pCenters(this.classCenters(this.pClasses(i))).sum(this.points(i))
      this.exceptPCenters(i) = vZero
    }

    for{i <- It.range(0, this.points.size)
        j <- It.range(0, this.points.size)
        if i != j
        if this.classCenters(this.pClasses(i)) == this.classCenters(this.pClasses(j)) } {
      this.exceptPCenters(i) = this.exceptPCenters(i).sum(this.points(j))
    }
  }

  def GAP = {
    updatePointsStats
    (this.cGAP.sum)/(this.cGAP.size)
  }

  def leafsGAP = {
    if(this.children.size > 0)
      this.children.map(c => c.clusteringGAP).sum
    else
      this.GAP match {
        case v if v.isInfinite || v.isNaN => 0.0
        case v => v
      }
  }
/*  def getBinaryClasses() = {
    val base = It.range(0, deep).map(d => Math.pow(2, deep).toInt).sum + left * 2
    (base, base + 1)
  }
  def getBinaryChildPositons (deep:Int, left:Int) = {
    ((deep + 1, 2 * left), (deep + 1, 2 * left + 1)) 
  }*/
} 
object ClusteringNode {
  def apply(encoded:EncodedNode):ClusteringNode = {
    val ret = ClusteringNode(name = encoded.name, tokens = encoded.tokens.clone, points = encoded.points.clone, pClasses = encoded.pClasses.clone
      , params = encoded.params
      , maxTopWords = encoded.deserialize[Int]("maxTopWords")
      , classCenters = encoded.classCenters
      , vectorSize = encoded.deserialize[Int]("vectorSize")
    )
    ret.hits = encoded.hits
    ret.stepHits = encoded.stepHits
    ret.initializing = encoded.deserialize[Boolean]("initializing")
    encoded.deserialize[Array[MLVector]]("pCenters").zipWithIndex.foreach{case (v, i) => ret.pCenters(i) = v}
    encoded.deserialize[Array[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    encoded.deserialize[Array[Double]]("cGAP").zipWithIndex.foreach{case (v, i) => ret.cGAP(i) = v}
    encoded.deserialize[Array[MLVector]]("exceptPCenters").zipWithIndex.foreach{case (v, i) => ret.exceptPCenters(i) = v}
    encoded.deserialize[Array[Double]]("pScores").zipWithIndex.foreach{case (v, i) => ret.pScores(i) = v}
    ret 
  }
}


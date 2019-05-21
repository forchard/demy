package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}

case class ClusteringNode (
  params:NodeParams
  , points: ArrayBuffer[MLVector]
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  assert(!this.params.maxTopWords.isEmpty)
  assert(!this.params.classCenters.isEmpty)
  assert(!this.params.vectorSize.isEmpty)
  assert(!this.params.childSplitSize.isEmpty)
  val maxTopWords = this.params.maxTopWords.get
  val classCenters = this.params.classCenters.get.map{case (classStr, center) => (classStr.toInt, center)}
  val vectorSize = this.params.vectorSize.get
  val pScores = this.params.pScores.getOrElse(Array.fill(maxTopWords)(0.0))
  val childSplitSize = this.params.childSplitSize.get
  val classCentersSet = classCenters.groupBy{case (cla, center) => center}.mapValues(p => p.map{case (cla, center) => cla}.toSet)

  var initializing = true
  val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  val pCenters = Array.fill(this.classCenters.values.toSet.size)(vZero)
  val vCenters = Array.fill(this.classCenters.values.toSet.size)(vZero)
  val cGAP = Array.fill(this.classCenters.values.toSet.size)(1.0)
  val exceptPCenters = Array.fill(maxTopWords)(vZero)
  val cHits = Array.fill(this.classCenters.values.toSet.size)(0.0)
  
  def encodeExtras(encoder:EncodedNode) {
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
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]], scores:Option[HashMap[Int, HashMap[Int, Double]]]=None, vectors:Seq[MLVector], tokens:Seq[String], cGeneratror:Iterator[Int]) { 
    if(this.hits > this.childSPlitSize && this.children.size < this.pCenters.size)
      this.fillChildren(cGenerator)
    }
    for{inClass <- this.links.keySet.iterator
      (iBase, _) <- facts.get(inClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()) } {
      
      val (outClass, score) = this.score(vectors(iBase), tokens(iBase), inClass)
      if(score > 0.5) {
        facts.get(outClass) match {
          case Some(f) => f(iBase) = iBase
          case None => facts(outClass) = HashMap(iBase -> iBase)
        }
        scores match {
          case Some(theScores) => {
            theScores.get(outClass) match {
              case Some(s) => s(iBase) = score
              case None => theScores(outClass) = HashMap(iBase -> score)
            }
          }
          case None =>
        }
      }
    }
  }

  def onInit(vector:MLVector, token:String, inClass:Int) = {
    if(initializing) {
      //println(s">>>>> ${this.hits} --> checking... $inClass ($token), ${this.params.links(inClass)} diff  ${this.pClasses.toSet}")
      val asClass = 
        this.outClasses.diff(this.rel.keySet).headOption match {
          case Some(classToFill) if 
            this.links(inClass).iterator
              .flatMap(outClass => this.rel.get(outClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()))
              .filter{case(iOut, _) => this.points(iOut).similarityScore(vector) > 0.999}
              .size == 0 
           =>
            this.tokens += token
            this.points += vector
            this.rel.get(classToFill) match {
              case Some(r) => r(this.tokens.size-1) = inClass
              case None => this.rel(classToFill) = HashMap(this.tokens.size -1 -> inClass)
            }
            //println(s"init pClasses: ${this.pClasses.toSeq}")
            classToFill
          case Some(classToFill) =>
              this.links(inClass).iterator
                .flatMap(outClass => this.rel.get(outClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()).map(p => (p, outClass)))
                .filter{case((iOut, _), outCLass) => this.points(iOut).similarityScore(vector) > 0.999}
                .next match { case ((iOut, _), outClass) => outClass}
          case _ => -1
      }
      initializing = this.rel.values.toSet != this.links.values.toSet
      updatePointsStats
      //println(s"<<<<< ${this.hits} --> checking... $inClass, ${this.params.links(inClass)} diff  ${this.pClasses.toSet}")
      asClass
    }
    else
      -1
  }
  
  def score(vector:MLVector, token:String, inClass:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None) = {
    val (vClass, iCenter, cSimilarity) =  
    (onInit(vector, token, inClass) match {
      case forced if(forced >= 0) => Set(forced) 
      case _ => this.links(inClass) 
    })
      .map(outClass => (outClass, this.classCenters(outClass), vector.similarityScore(this.pCenters(this.classCenters(outClass)))))
      .reduce((t1, t2) => (t1, t2) match {
        case ((class1, iCenter1, score1), (class2, iCenter2, score2)) => 
          if(score1 > score2 && !score1.isNaN || score2.isNaN) t1 
          else t2
      })
    val (iPoint, pSimilarity) = 
      (for(i <- this.rel(vClass).keysIterator ) 
         yield (i, vector.similarityScore(this.points(i)))
      ).reduce((p1, p2) => (p1, p2) match {
          case ((i1, score1), (i2, score2)) => 
            if(score1 > score2) p1 
            else p2
      })
    this.fitPoint(vector = vector, token= token, vClass = vClass, vScore = pSimilarity, iPoint = iPoint, iCenter=iCenter, weight = weight, asVCenter = asVCenter)
    (vClass, pSimilarity)
  }

  def fitPoint(vector:MLVector, token:String, vClass:Int, vScore:Double, iPoint:Int, iCenter:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None) {
    var str = ""
    this.pScores(iPoint) = this.pScores(iPoint) + vScore * weight
    if(str.size > 0) println(str)
    this.vCenters(iCenter) = asVCenter match {
      case Some(v) => this.vCenters(iCenter).scale(cHits(iCenter) + 1.0).sum(v.scale(weight)).scale(1.0/(cHits(iCenter) + 1 + weight))
      case None    => this.vCenters(iCenter).scale(cHits(iCenter) + 1.0).sum(vector.scale(weight)).scale(1.0/(cHits(iCenter) + 1 + weight))
    }
    if(this.tokens(iPoint) != token && !this.initializing)
      tryAsPoint(vector = vector, token = token, vClass = vClass, iPoint = iPoint, iCenter = iCenter)
    this.cGAP(iCenter) = 1.0 - this.pCenters(iCenter).similarityScore(this.vCenters(iCenter))
    cHits(iCenter) = cHits(iCenter) + weight
  }
  def pointWeight(i:Int) = {
    (pScores(i)
      / (It.range(0, this.points.size)
        //.filter(j => pClasses(i) == pClasses(j))
        .map(j => pScores(j))
        .sum)
     )
  }
  def estimatePointHits(i:Int) = {
    val ret = pointWeight(i)*this.params.hits//cHits(classCenters(pClasses(i)))
    ret
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
          this.rel(vClass)(this.tokens.size-1) = this.tokens.size-1
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
    for(center <- It.range(0, this.pCenters.size)) 
      this.pCenters(center) = 
        this.classCentersSet(center).iterator
          .flatMap(cClass => 
              this.rel.get(cClass).map(r => r.iterator).getOrElse(It[(Int, Int)]())
                .map{case (i, _) => this.points(i)}
          ).fold(vZero)((current, next) => (current.sum(next)))
    
    for{center <- It.range(0, this.pCenters.size) 
         (i, _) <- 
           this.classCentersSet(center).iterator
             .flatMap(cClass => this.rel.get(cClass).map(r => r.iterator).getOrElse(It[(Int, Int)]()))} {
      this.exceptPCenters(i) = this.pCenters(center).minus(this.points(i))
    }

    
    //println(s"init pClasses: ${this.pClasses.toSeq} (${this.points.size})")
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
  def fromClass(toClass:Int) =  
    this.links
      .toSeq
      .flatMap{case (from, to) => 
        if(to(toClass)) Some(from)
        else None
      }
      .head

  def mergeWith(thatNode:Node):this.type = {
    thatNode match {
      case that:ClusteringNode =>
        that.links.keys.foreach{ inClass =>
          that.collectLeafPoints(inClass = inClass)
            .foreach{case(lVector, lToken, lHits, lCenter) =>
              this.mergeChildren(inClass, lVector, lToken, lHits, lCenter)
            }
        }
      case _ => throw new Exception("Clustering node cannot learn from ${o.getClass.gertName}")
    }
    this
  }
  def mergeChildren(inClass:Int, vector:MLVector, token:String, weight:Double, center:MLVector)  {
    this.params.hits = this.params.hits + weight
    val (bestClass, bestScore) = this.score(
      vector = vector
      , token = token
      , inClass = inClass
      , weight= weight
      , asVCenter = Some(center)
    )
    for{i <- It.range(0, this.children.size)
        if(this.children(i).links.keySet(bestClass))
    } this.children(i).asInstanceOf[ClusteringNode].mergeChildren(bestClass, vector, token, weight, center)
  }
  def resetHitsExtras {
    It.range(0, this.pScores.size).foreach(i => pScores(i) = 0.0)
    It.range(0, this.cHits.size).foreach(i => cHits(i) = 0.0)
  }

  def collectLeafPoints(inClass:Int, buffer:ArrayBuffer[(MLVector, String, Double, MLVector)]=ArrayBuffer[(MLVector, String, Double, MLVector)]()): ArrayBuffer[(MLVector, String, Double, MLVector)]= {
    if(this.children.size == 0 || this.children.filter(c => c.params.hits == 0).size > 0)
      for{outClass <- this.links(inClass).iterator 
          (i, _) <- this.rel.get(outClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()) } {
        buffer += ((this.points(i), this.tokens(i), this.estimatePointHits(i), this.vCenters(this.classCenters(outClass))))
      }
    else 
      for{c <- It.range(0, this.children.size)} {
        this.links(inClass)
          .intersect(this.children(c).inClasses)
          .foreach(childInClass => this.children(c).asInstanceOf[ClusteringNode].collectLeafPoints(inClass = childInClass, buffer))
      }
    buffer
  }
} 
object ClusteringNode {
  def apply(params:NodeParams, index:VectorIndex):ClusteringNode = {
    val ret = ClusteringNode(
      points = ArrayBuffer[MLVector]() 
      , params = params
    )
    ret.points ++= (index(ret.tokens) match {case map => ret.tokens.map(t => map(t))}) 
    ret 
  }
  def apply(encoded:EncodedNode):ClusteringNode = {
    val ret = ClusteringNode(
      points = encoded.points
      , params = encoded.params
    )
    ret.initializing = encoded.deserialize[Boolean]("initializing")
    encoded.deserialize[Array[MLVector]]("pCenters").zipWithIndex.foreach{case (v, i) => ret.pCenters(i) = v}
    encoded.deserialize[Array[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    encoded.deserialize[Array[Double]]("cGAP").zipWithIndex.foreach{case (v, i) => ret.cGAP(i) = v}
    encoded.deserialize[Array[MLVector]]("exceptPCenters").zipWithIndex.foreach{case (v, i) => ret.exceptPCenters(i) = v}
    encoded.deserialize[Array[Double]]("pScores").zipWithIndex.foreach{case (v, i) => ret.pScores(i) = v}
    encoded.deserialize[Array[Double]]("cHits").zipWithIndex.foreach{case (v, i) => ret.cHits(i) = v}
    ret 
  }
}

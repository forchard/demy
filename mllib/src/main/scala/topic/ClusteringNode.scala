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
  val classCenters = HashMap(this.params.classCenters.get.map{case (classStr, center) => (classStr.toInt, center)}.toSeq :_*)
  val vectorSize = this.params.vectorSize.get
  val pScores = if(this.params.annotations.size == maxTopWords) Array(this.params.annotations.map(a => a.score) :_*) else Array.fill(maxTopWords)(0.0)
  val cError = this.params.cError.getOrElse(Array.fill(this.classCenters.values.toSet.size)(0.0))
  val childSplitSize = this.params.childSplitSize.get
  val classCentersMap = classCenters.groupBy{case (cla, center) => center}.mapValues(p => p.map{case (cla, center) => cla}.toSet)

  var initializing = true
  val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  val pCenters = ArrayBuffer.fill(this.classCenters.values.toSet.size)(vZero)
  val vCenters = ArrayBuffer.fill(this.classCenters.values.toSet.size)(vZero)
  val cGAP = ArrayBuffer.fill(this.classCenters.values.toSet.size)(1.0)
  val exceptPCenters = ArrayBuffer.fill(maxTopWords)(vZero)
  val cHits = ArrayBuffer.fill(this.classCenters.values.toSet.size)(0.0)
  
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
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:Option[HashMap[Int, HashMap[Int, Double]]]=None
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGenerator:Iterator[Int]) {
    for{inClass <- this.links.keySet.iterator
      (iBase, _) <- facts.get(inClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()) } {
      
        val (outClass, score) = 
          this.score(
            vector = vectors(iBase)
            , token = tokens(iBase)
            , inClass = inClass 
            , weight = 1.0
            , asVCenter = None
            , parent = parent match {case Some(c) => c match {case c:ClusteringNode => Some(c) case _ => None} case _ => None}
            , cGenerator = cGenerator
          )
      if(true) {
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
      val asClass = 
        this.links(inClass).diff(this.rel.keySet).headOption match {
          case Some(classToFill) if 
            this.links(inClass).iterator
              .flatMap(outClass => this.rel.get(outClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()))
              .filter{case(iOut, _) => this.points(iOut).similarityScore(vector) > 0.999}
              .size == 0 
           =>
            this.tokens += token
            this.points += vector
            this.rel.get(classToFill) match {
              case Some(r) => r(this.tokens.size-1) = this.tokens.size-1
              case None => {
                this.rel(classToFill) = HashMap(this.tokens.size -1 -> (this.tokens.size -1))
                this.inRel(classToFill) = HashMap((this.tokens.size -1 -> (this.tokens.size -1), true))
              }
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
      initializing = this.rel.keySet != this.links.values.flatMap(v => v).toSet
      updatePointsStats
      //println(It.range(1, 3).map(t => s"($t) ${this.classPath.filter{case(cat, parents) => parents(t)}.map{case (cat, _) => this.rel.get(cat) match { case Some(values) => values.map{case (iClass, _) => this.tokens(iClass)}.mkString(",") case None => "-" }}} ").mkString("<---->"))
      asClass
    }
    else
      -1
  }
  def clusterScore = if(cHits.sum == 0) 0.0 else 1.0 - It.range(0, this.cError.size).map(i => cError(i)*cHits(i)).sum / cHits.sum 
  def clusterBalance = if(cHits.sum == 0) 0.0 else {
    val sum = cHits.sum
    val avg = sum / cHits.size
    val excedent = It.range(0, this.cHits.size).map(i => Math.abs(avg - cHits(i))).sum / 2.0
    val maxExcedent = sum - avg
    val imbalance = excedent / maxExcedent
    1.0 - imbalance
  }
  def centerBoostFactor(iCenter:Int) = if(cHits.sum == 0) 1.0 else {
    val total = cHits.sum
    val share = cHits(iCenter)/total
    val boost = cHits.size * (1.0 - share)
    boost
  }
  def round(v:Double) = {
    import scala.math.BigDecimal
    BigDecimal(v).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble 
  }
  def score(vector:MLVector, token:String, inClass:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None, parent:Option[ClusteringNode]=None, cGenerator: Iterator[Int]) = {
    val vectorOrCenter = asVCenter match {case Some(v) => v case None => vector } 
    if(this.children.size < this.classCentersMap.size
        && !this.initializing
        && this.pScores.sum > this.childSplitSize
        && (parent.isEmpty ||  this.clusterScore < 0.9) /*(this.clusterScore - parent.get.clusterScore)/parent.get.clusterScore > 0.02 )*/ ) {
      //println(s"spawning... ${this.params.annotations}, ${this.inClasses}")
      this.fillChildren(cGenerator)
    }
    val (vClass, iCenter, cSimilarity) =  
    (onInit(vector, token, inClass) match {
      case forced if(forced >= 0) => Set(forced) 
      case _ => this.links(inClass) 
    })
      .map(outClass => (outClass, this.classCenters(outClass), vectorOrCenter.similarityScore(this.pCenters(this.classCenters(outClass)))*this.centerBoostFactor(this.classCenters(outClass))))
      .reduce((t1, t2) => (t1, t2) match {
        case ((class1, iCenter1, score1), (class2, iCenter2, score2)) => 
          if(score1 > score2 && !score1.isNaN || score2.isNaN) t1 
          else t2
      })
    val (iPoint, pSimilarity) = 
      (for(i <- this.rel(vClass).keysIterator ) 
         yield (i, vectorOrCenter.similarityScore(this.points(i)))
      ).reduce((p1, p2) => (p1, p2) match {
          case ((i1, score1), (i2, score2)) => 
            if(score1 > score2) p1 
            else p2
      })
    this.fitPoint(vector = vector, token = token, vClass = vClass, vScore = pSimilarity, iPoint = iPoint, iCenter = iCenter, weight = weight, asVCenter = asVCenter)
    (vClass, pSimilarity)
  }

  def fitPoint(vector:MLVector, token:String, vClass:Int, vScore:Double, iPoint:Int, iCenter:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None) {
    var str = ""
    this.pScores(iPoint) = this.pScores(iPoint) + vScore * weight
    val vectorOrCenter =  asVCenter match {case Some(v) => v case _ => vector}
    this.vCenters(iCenter) = this.vCenters(iCenter).scale(cHits(iCenter) + 1.0).sum(vectorOrCenter.scale(weight)).scale(1.0/(cHits(iCenter) + 1 + weight))

    if(this.tokens(iPoint) != token && !this.initializing)
      tryAsPoint(vector = vector, token = token, vClass = vClass, iPoint = iPoint, iCenter = iCenter)
    this.cGAP(iCenter) = 1.0 - this.pCenters(iCenter).similarityScore(this.vCenters(iCenter))
    this.cError(iCenter) = (this.cError(iCenter) * (cHits(iCenter) + 1.0) + (1.0 - vectorOrCenter.similarityScore(pCenters(iCenter))) * weight ) / (cHits(iCenter) + 1 + weight)
    cHits(iCenter) = cHits(iCenter) + weight
  }
  def pointWeight(i:Int) = {
    (pScores(i)
      / (It.range(0, this.points.size)
        .map(j => pScores(j))
        .sum)
     )
  }
  def estimatePointHits(i:Int) = {
    val ret = pointWeight(i)*this.params.hits
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
          this.inRel(vClass)(this.tokens.size-1 -> (this.tokens.size-1)) = true
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
      thights.tokens.remove(viPoint)
      this.pClasses.remove(viPoint)
      updatePointsStats
    }*/
  }

  def updatePointsStats {
    for(center <- It.range(0, this.pCenters.size)) 
      this.pCenters(center) = 
        this.classCentersMap(center).iterator
          .flatMap(cClass => 
              this.rel.get(cClass).map(r => r.iterator).getOrElse(It[(Int, Int)]())
                .map{case (i, _) => this.points(i)}
          ).fold(vZero)((current, next) => (current.sum(next)))
    
    for{center <- It.range(0, this.pCenters.size) 
         (i, _) <- 
           this.classCentersMap(center).iterator
             .flatMap(cClass => this.rel.get(cClass).map(r => r.iterator).getOrElse(It[(Int, Int)]()))} {
      this.exceptPCenters(i) = this.pCenters(center).minus(this.points(i))
    }
    
    this.updateParams()
    
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

  def mergeWith(thatNode:Node, cGenerator:Iterator[Int]):this.type = {
    var str = ""
    val print = thatNode.inClasses == Set(1, 2)
    thatNode match {
      case that:ClusteringNode =>
        that.links.keys.foreach{ inClass =>
          that.collectLeafPoints(inClass = inClass)
            .foreach{case(lVector, lToken, lHits, lCenter) =>
              this.mergeChildren(inClass, lVector, lToken, lHits, lCenter, None, cGenerator)
            }
        }
      case _ => throw new Exception(s"Clustering node cannot learn from ${thatNode.getClass.getName}")
    }
    this
  }
  def mergeChildren(inClass:Int, vector:MLVector, token:String, weight:Double, center:MLVector, parent:Option[ClusteringNode], cGenerator:Iterator[Int])  {
    this.params.hits = this.params.hits + weight
    val (bestClass, bestScore) = 
      this.score(
        vector = vector
        , token = token
        , inClass = inClass
        , weight= weight
        , asVCenter = Some(center)
        , parent
        , cGenerator
    )
    for{i <- It.range(0, this.children.size)
        if(this.children(i).links.keySet(bestClass))
    } this.children(i).asInstanceOf[ClusteringNode].mergeChildren(bestClass, vector, token, weight, center, Some(this), cGenerator)
  }
  def resetHitsExtras {
    It.range(0, this.pScores.size).foreach(i => pScores(i) = 0.0)
    It.range(0, this.cHits.size).foreach(i => cHits(i) = 0.0)
  }
  def cloneUnfittedExtras = this.params.cloneWith(classMapping = None, unFit = true).get.toNode().asInstanceOf[this.type]
  def updateParamsExtras {
    this.params.cError = Some(this.cError.clone)
  } 
  def collectLeafPoints(inClass:Int, buffer:ArrayBuffer[(MLVector, String, Double, MLVector)]=ArrayBuffer[(MLVector, String, Double, MLVector)]()): ArrayBuffer[(MLVector, String, Double, MLVector)]= {
    if(this.children.size == 0 || this.children.filter(c => c.asInstanceOf[ClusteringNode].pScores.sum == 0).size > 0)
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

  def fillChildren(cGenerator:Iterator[Int]) {
    for{i <- It.range(this.children.size, this.classCenters.values.toSet.size) if cGenerator.hasNext } { 
      val fromMap = 
        this.classCenters.flatMap{ case (outClass, iCenter) => 
          if(iCenter == i) 
            Some((this.links.flatMap{case (from, toSet) => if(toSet(outClass)) Some(from) else None}.head, outClass)) 
          else None
        }.toMap
      val toMap = this.outClasses.iterator.zip(cGenerator).toSeq.toMap
      this.children ++=  this.params.cloneWith(classMapping = Some(fromMap ++ toMap), unFit = true).map(p => p.toNode())
      val hitDiff = this.params.hits - It.range(0, this.children.size).map(i => this.children(i).params.hits).sum
      val initHits = hitDiff /  It.range(0, this.children.size).filter(i => this.children(i).params.hits == 0).size
      It.range(0, this.children.size).filter(i => this.children(i).params.hits == 0).foreach(i => this.children(i).params.hits = initHits)
    }
    if(this.children.size < this.classCenters.values.toSet.size) this.children.clear
    //println(s"My hits = ${this.params.hits} ==> New hits ${this.children.map(c => c.params.hits).sum}")
  }
} 
object ClusteringNode {
  def apply(params:NodeParams, index:Option[VectorIndex]):ClusteringNode = {
    val ret = ClusteringNode(
      points = ArrayBuffer[MLVector]() 
      , params = params
    )
    if(ret.tokens.size > 0)
      ret.points ++= (index.get(ret.tokens) match {case map => ret.tokens.map(t => map(t))}) 
    ret 
  }
  def apply(encoded:EncodedNode):ClusteringNode = {
    val ret = ClusteringNode(
      points = encoded.points
      , params = encoded.params
    )
    ret.initializing = encoded.deserialize[Boolean]("initializing")
    encoded.deserialize[ArrayBuffer[MLVector]]("pCenters").zipWithIndex.foreach{case (v, i) => ret.pCenters(i) = v}
    encoded.deserialize[ArrayBuffer[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cGAP").zipWithIndex.foreach{case (v, i) => ret.cGAP(i) = v}
    encoded.deserialize[ArrayBuffer[MLVector]]("exceptPCenters").zipWithIndex.foreach{case (v, i) => ret.exceptPCenters(i) = v}
    encoded.deserialize[Array[Double]]("pScores").zipWithIndex.foreach{case (v, i) => ret.pScores(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cHits").zipWithIndex.foreach{case (v, i) => ret.cHits(i) = v}
    ret 
  }
}

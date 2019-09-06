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
  val childSplitSize = this.params.childSplitSize.get
  val classCentersMap = classCenters.groupBy{case (cla, center) => center}.mapValues(p => p.map{case (cla, center) => cla}.toSet)

  val pScores = if(this.params.annotations.size == maxTopWords) Array(this.params.annotations.map(a => a.score) :_*) else Array.fill(maxTopWords)(0.0)
  val cError = this.params.cError.getOrElse(Array.fill(this.classCenters.values.toSet.size)(0.0))
  
  var initializing = true
  val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  val pCenters = ArrayBuffer.fill(this.classCenters.values.toSet.size)(vZero)
  val vCenters = ArrayBuffer.fill(this.classCenters.values.toSet.size)(vZero)
  val cGAP = ArrayBuffer.fill(this.classCenters.values.toSet.size)(1.0)
  val exceptPCenters = ArrayBuffer.fill(maxTopWords)(vZero)
  val cHits = ArrayBuffer.fill(this.classCenters.values.toSet.size)(0.0)
  
  def encodeExtras(encoder:EncodedNode) {
    //thiese two are not necessary if params are updated at encode time. Please remove after test
    encoder.serialized += (("pScores",serialize(pScores) ))
    encoder.serialized += (("cError",serialize(cError) ))

    encoder.serialized += (("initializing", serialize(initializing)))
    encoder.serialized += (("pCenters",serialize(pCenters )))
    encoder.serialized += (("vCenters",serialize(vCenters )))
    encoder.serialized += (("cGAP",serialize(cGAP )))
    encoder.serialized += (("exceptPCenters", serialize(exceptPCenters)))
    encoder.serialized += (("cHits",serialize(cHits) ))
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> classCenters: ${classCenters}\n")
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> pCenters: ${pCenters.size}\n")
    buffer
  }
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:HashMap[Int, Double]
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGenerator:Iterator[Int]
      , fit:Boolean) {
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
            , fit = fit
          )
        facts.get(outClass) match {
          case Some(f) => f(iBase) = iBase
          case None => facts(outClass) = HashMap(iBase -> iBase)
        }
    }
    for{(inClass, outClass) <- this.linkPairs} {
        scores(outClass) = this.getClassScore(vectors=vectors, facts = facts, inClass = inClass, outClass = outClass)
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
//    if(this.links.contains(1))
//      println(s"$total, $share, $boost")
    boost
  }
  def round(v:Double) = {
    import scala.math.BigDecimal
    BigDecimal(v).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble 
  }

  def getClassScore(vectors:Seq[MLVector], facts:HashMap[Int, HashMap[Int, Int]], inClass:Int, outClass:Int) = {
    /*if(facts.contains(inClass)) {
      for{i <- facts(inClass).keysIterator } {
        sum = sum.sum(vectors(i))
      }
    }*/
   val sum = vectors.filter(_ != null).reduceOption((v1, v2) => v1.sum(v2)).getOrElse(vZero)
   sum.similarityScore(pCenters(this.classCenters(outClass)))
  }
  def score(vector:MLVector, token:String, inClass:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None, parent:Option[ClusteringNode]=None, cGenerator: Iterator[Int], fit:Boolean) = {
    val vectorOrCenter = asVCenter match {case Some(v) => v case None => vector } 
    if(fit
        && cGenerator.hasNext 
        && this.children.size < this.classCentersMap.size
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
    this.affectPoint(vector = vector, token = token, vClass = vClass, vScore = pSimilarity, iPoint = iPoint, iCenter = iCenter, weight = weight, asVCenter = asVCenter, fit = fit)
    (vClass, pSimilarity)
  }

  def affectPoint(vector:MLVector, token:String, vClass:Int, vScore:Double, iPoint:Int, iCenter:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None, fit:Boolean) {
    this.pScores(iPoint) = this.pScores(iPoint) + vScore * weight
    val vectorOrCenter =  asVCenter match {case Some(v) => v case _ => vector}
    this.vCenters(iCenter) = this.vCenters(iCenter).scale(cHits(iCenter) + 1.0).sum(vectorOrCenter.scale(weight)).scale(1.0/(cHits(iCenter) + 1 + weight))

    if(String.CASE_INSENSITIVE_ORDER.compare(this.tokens(iPoint),token) != 0  && !this.initializing && fit)
      tryAsPoint(vector = vector, token = token, vClass = vClass, iPoint = iPoint, iCenter = iCenter)
    this.cGAP(iCenter) = 1.0 - this.pCenters(iCenter).similarityScore(this.vCenters(iCenter))
    this.cError(iCenter) = (this.cError(iCenter) * (cHits(iCenter) + 1.0) + (1.0 - vectorOrCenter.similarityScore(pCenters(iCenter))) * weight ) / (cHits(iCenter) + 1 + weight)
    cHits(iCenter) = cHits(iCenter) + weight
  }
  def pointWeight(i:Int) = {
    val sum = (It.range(0, this.points.size)
        .map(j => pScores(j))
        .sum)
    if(sum == 0.0) 0.0 else (pScores(i)/sum)
  }
  def estimatePointHits(i:Int) = {
    val ret = pointWeight(i)*this.params.hits
    ret
  }
  def tryAsPoint(vector:MLVector, token:String, vClass:Int, iPoint:Int, iCenter:Int) {
    var viPoint = iPoint
    //Evaluating replacing the closest (and given) point
    val newPCenter = this.exceptPCenters(viPoint).sum(vector)
    if((1.0 - newPCenter.similarityScore(this.vCenters(iCenter))) - this.cGAP(iCenter) < 0.0001) {
      //println(s"$step gap: ${this.cGAP(iCenter)} replacing $token by ${this.tokens(viPoint)}")
      this.points(viPoint) = vector
      this.tokens(viPoint) = token
      updatePointsStats
    } else {
      if(this.points.size < this.maxTopWords) {
        //Evaluating adding as a new Center
        val newPCenter = this.pCenters(iCenter).sum(vector)
        if((1.0 - newPCenter.similarityScore(this.vCenters(iCenter))) - this.cGAP(iCenter) < 0.0001) {
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
    
    this.updateParams(None, false)
    
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

  def mergeWith(thatNode:Node, cGenerator:Iterator[Int], fit:Boolean):this.type = {
    thatNode match {
      case that:ClusteringNode =>
        if(fit) {
          this.collectLeafPoints(fromNode = Some(that))
            .foreach{case(inClass, lVector, lToken, lHits, lCenter) =>
              if(lHits > 0.00001)
                this.mergeChildren(inClass, lVector, lToken, lHits, lCenter, None, cGenerator)
            }
        }
        else {
          this.params.hits = this.params.hits + that.params.hits
          It.range(0, this.pScores.size).foreach(i => this.pScores(i) =  this.pScores(i) + that.pScores(i) )
          It.range(0, this.cHits.size).foreach(i => this.cHits(i) =  this.cHits(i) + that.cHits(i))
          It.range(0, this.children.size).foreach(i => this.children(i).mergeWith(that.children(i), cGenerator, fit))
          this
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
        , true
    )
    for{i <- It.range(0, this.children.size)
        if(this.children(i).params.filterValue.contains(bestClass))
    } this.children(i).asInstanceOf[ClusteringNode].mergeChildren(inClass, vector, token, weight, center, Some(this), cGenerator)
  }
  def resetHitsExtras {
    It.range(0, this.pScores.size).foreach(i => pScores(i) = 0.0)
    It.range(0, this.cHits.size).foreach(i => cHits(i) = 0.0)
  }
  def cloneUnfittedExtras = this.params.cloneWith(classMapping = None, unFit = true).get.toNode().asInstanceOf[this.type]
  def updateParamsExtras {
    this.params.cError = Some(this.cError.clone)
  } 
  def collectLeafPoints(fromNode:Option[ClusteringNode]=None, buffer:ArrayBuffer[(Int, MLVector, String, Double, MLVector)]=ArrayBuffer[(Int, MLVector, String, Double, MLVector)]() 
  ) : ArrayBuffer[(Int, MLVector, String, Double, MLVector)]= {
    val from = fromNode.getOrElse(this)
    if(from.children.size == 0 || from.children.filter(c => c.asInstanceOf[ClusteringNode].pScores.sum == 0).size > 0)
      for{
        thisIn <- this.links.keysIterator
        (itIn, itOut) <- from.links.iterator.flatMap{case (in, outSet) => outSet.iterator.map(o => (in, o))}
            if from.classPath(itOut).contains(thisIn)
        (i, _) <- from.rel.get(itOut).map(o => o.iterator).getOrElse(It[(Int, Int)]()) } {
        buffer += ((thisIn, from.points(i), from.tokens(i), from.estimatePointHits(i), from.vCenters(from.classCenters(itOut))))
      }
    else 
      for(c <- It.range(0, from.children.size)) {
          this.collectLeafPoints(fromNode = Some(from.children(c).asInstanceOf[ClusteringNode]), buffer)
      }
    buffer
  }

  def fillChildren(cGenerator:Iterator[Int]) {
    for{i <- It.range(this.children.size, this.classCenters.values.toSet.size) if cGenerator.hasNext } { 
      val fromMap = 
        this.classCenters.flatMap{ case (outClass, iCenter) => 
          if(iCenter == i) 
            this.links.flatMap{case (from, toSet) => if(toSet(outClass)) Some(from) else None}.head match {case from => Some(from, outClass)} 
          else None
        }.toMap
      val toMap = this.outClasses.iterator.zip(cGenerator).toSeq.toMap
      val filterMap = 
        this.classCenters
          .filter{ case (outClass, iCenter) => iCenter == i}
          .zipWithIndex
          .map{case ((outClass, iCenter), j) => (this.params.filterValue(j), outClass)}
          .toMap
      this.children ++=  this.params.cloneWith(classMapping = Some(fromMap ++ toMap ++ filterMap), unFit = true).map(p => p.toNode())
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
    //these two are not necessary if params are updated at encode time comment after test
    encoded.deserialize[Array[Double]]("pScores").zipWithIndex.foreach{case (v, i) => ret.pScores(i) = v}
    encoded.deserialize[Array[Double]]("cError").zipWithIndex.foreach{case (v, i) => ret.cError(i) = v}
    
    ret.initializing = encoded.deserialize[Boolean]("initializing")
    encoded.deserialize[ArrayBuffer[MLVector]]("pCenters").zipWithIndex.foreach{case (v, i) => ret.pCenters(i) = v}
    encoded.deserialize[ArrayBuffer[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cGAP").zipWithIndex.foreach{case (v, i) => ret.cGAP(i) = v}
    encoded.deserialize[ArrayBuffer[MLVector]]("exceptPCenters").zipWithIndex.foreach{case (v, i) => ret.exceptPCenters(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cHits").zipWithIndex.foreach{case (v, i) => ret.cHits(i) = v}
    ret 
  }
}

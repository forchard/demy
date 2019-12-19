package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap, ListBuffer}
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
  val numCenters = this.classCenters.values.toSet.size
  val cError = this.params.cError.getOrElse(Array.fill(numCenters)(0.0))
  
  var initializing = true
  val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  val vCenters = ArrayBuffer.fill(maxTopWords)(vZero)
  val pGAP = ArrayBuffer.fill(maxTopWords)(1.0)
  val cHits = ArrayBuffer.fill(numCenters)(0.0)
  

  def encodeExtras(encoder:EncodedNode) {
    //thiese two are not necessary if params are updated at encode time. Please remove after test
    encoder.serialized += (("pScores",serialize(pScores) ))
    encoder.serialized += (("cError",serialize(cError) ))
    encoder.serialized += (("initializing", serialize(initializing)))
    encoder.serialized += (("vCenters",serialize(vCenters )))
    encoder.serialized += (("pGAP",serialize(pGAP )))
    encoder.serialized += (("cHits",serialize(cHits) ))
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> classCenters: ${classCenters}\n")
    buffer
  }
  
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:HashMap[Int, Double]
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGenerator:Iterator[Int]
      , fit:Boolean) {

      
    val vectorsInScopeCount =  this.links.keysIterator.map(inClass => facts.get(inClass).map(o => o.size).getOrElse(0)).sum

    val scoresByClass = 
      for(inClass <- this.links.keySet.iterator)
        yield {
          (inClass, 
            for((iBase, _) <- facts.get(inClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()))
              yield {
                val scoredPoint = 
                  this.score(
                    iVector = iBase
                    , vectors = vectors
                    , vTokens = tokens
                    , inClass = inClass 
                    , parent = parent match {case Some(c) => c match {case c:ClusteringNode => Some(c) case _ => None} case _ => None}
                    , cGenerator = cGenerator
                    , fit = fit
                  )
                scoredPoint
              }
          )
        }


    val sequenceScore = this.scoreSequence(scoredTokens = scoresByClass.map{case (inClass, scores) => (inClass, scores.flatMap(s => s))})

    sequenceScore.map{case ScoredSequence(inClass, outClass, score, scoredVectors) => 
      scores(outClass) = score

      for(ScoredVector(iVector, outVectorClass, outVectorScore, iPoint, iCenter) <- scoredVectors ) {
        facts.get(outClass) match {
          case Some(f) => f(iVector) = iVector
          case None => facts(outClass) = HashMap(iVector -> iVector)
        }
        this.affectPoint(
          vector = vectors(iVector)
          , tokens = Seq(tokens(iVector))
          , vClass = outClass
          , vScore = outVectorScore
          , iPoint = iPoint
          , iCenter = iCenter
          , weight = 1.0 / vectorsInScopeCount
          , asVCenter = None
          , fit = fit
        )
      }
    }.size
  }

  def onInit(vector:MLVector, tokens:Seq[String], inClass:Int) = {
    if(initializing && this.points.size < this.maxTopWords) {
       this.links(inClass).iterator
         .flatMap(outClass => this.rel.get(outClass).map(o => o.iterator).getOrElse(It[(Int, Int)]()).map(p => (p, outClass)))
         .filter{case((iOut, _), outClass) => this.points(iOut).similarityScore(vector) > 0.999}
         .toSeq.headOption
         .map{case ((iOut, _), outClass) => outClass}
         match {
           case Some(classToFill) => classToFill
           case None =>
             val classToFill = this.links(inClass).map(outClass => (outClass, this.rel.get(outClass).map(_.size).getOrElse(0))).toSeq.sortWith(_._2 < _._2).head._1
             this.sequences += tokens
             this.points += vector
             this.rel.get(classToFill) match {
               case Some(r) => 
                 this.rel(classToFill)(this.sequences.size-1) = this.sequences.size-1
                 this.inRel(classToFill)(this.sequences.size-1 -> (this.sequences.size-1)) = true
               case None => 
                 this.rel(classToFill) = HashMap(this.sequences.size -1 -> (this.sequences.size -1))
                 this.inRel(classToFill) = HashMap((this.sequences.size -1 -> (this.sequences.size -1), true))
               
             }
             initializing = this.points.size < this.maxTopWords
             classToFill
         }
       //println(s"init pClasses: ${this.pClasses.toSeq}")
    }
      //println(It.range(1, 3).map(t => s"($t) ${this.classPath.filter{case(cat, parents) => parents(t)}.map{case (cat, _) => this.rel.get(cat) match { case Some(values) => values.map{case (iClass, _) => this.tokens(iClass)}.mkString(",") case None => "-" }}} ").mkString("<---->"))
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
    if(this.params.filterValue.contains(1))
      println(s"$iCenter, $total, $share, $boost")
    boost
  }
  def round(v:Double) = {
    import scala.math.BigDecimal
    BigDecimal(v).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble 
  }

  case class ScoredSequence(inClass:Int, outClass:Int, outScore:Double, scoredVectors:Iterator[ScoredVector])
  def scoreSequence(scoredTokens:Iterator[(Int, Iterator[ScoredVector])]) = {
    val pointScores = Array.fill(this.maxTopWords)(0.0)   
    val pointCounts = Array.fill(this.maxTopWords)(0)    

    val sequenceScore = 
     for((inClass, inClassScores)  <- scoredTokens)
        yield {
          
          val retTokens = this.outClasses.map(o => (o, ListBuffer[ScoredVector]())).toMap
          val (bestClass, bestScore) = {
            for(scoredToken  <- inClassScores) {

              pointScores(scoredToken.iPoint) = pointScores(scoredToken.iPoint) + scoredToken.outScore
              pointCounts(scoredToken.iPoint) = pointCounts(scoredToken.iPoint) + 1
              retTokens(scoredToken.outClass) += scoredToken 
            }
            
            this.links(inClass).iterator
              .map{outClass => 
                var vectorCount = 0
                val pointScore = 
                  this.rel.get(outClass).map(_.keysIterator).getOrElse(Iterator[Int]())
                    .map{iPoint => 
                      val pointScore = if(pointCounts(iPoint) == 0) 0.0 else pointScores(iPoint)/pointCounts(iPoint)
                      vectorCount = vectorCount + 1
                      pointScore
                    }
                    .reduceOption(_ + _).getOrElse(0.0)
                (outClass, if(pointScore == 0) 0.0 else pointScore / vectorCount)
              }
              .reduce((p1, p2) => (p1, p2) match {case ((_, score1),(_, score2)) => if(score1 > score2) p1 else p2})
          }

          ScoredSequence(inClass = inClass, outClass = bestClass, outScore = bestScore, scoredVectors = retTokens(bestClass).iterator)
        }
    sequenceScore
  }
  case class ScoredVector(iVector:Int, outClass:Int, outScore:Double, iPoint:Int, iCenter:Int)
  def score(
    iVector:Int
    , vectors:Seq[MLVector]
    , vTokens:Seq[String]
    , inClass:Int
    , parent:Option[ClusteringNode]=None
    , cGenerator: Iterator[Int]
    , fit:Boolean) = {
      
    //val vectorOrCenter = asVCenter match {case Some(v) => v case None => vectors(iVector) } 
    if(fit
        && cGenerator.hasNext 
        && this.children.size < this.classCentersMap.size
        && !this.initializing
        && this.pScores.sum > this.childSplitSize
        && (parent.isEmpty ||  this.clusterScore < 0.9) /*(this.clusterScore - parent.get.clusterScore)/parent.get.clusterScore > 0.02 )*/ ) {
      //println(s"spawning... ${this.params.annotations}, ${this.inClasses}")
      this.fillChildren(cGenerator)
    }
    onInit(vectors(iVector), Seq(vTokens(iVector)), inClass)
    
    this.links(inClass).iterator
      .filter{outClass => this.rel.contains(outClass)}
      .map{outClass => 
        val iCenter = this.classCenters(outClass)
        val (bestPoint, pSimilarity) = 
           (for(iPoint <- this.rel(outClass).keysIterator) 
             yield (iPoint, vectors(iVector).similarityScore(this.points(iPoint)))
           ) 
            .reduce((p1, p2) => (p1, p2) match {
              case ((i1, score1), (i2, score2)) => 
                if(score1 > score2) p1 
                else p2
            })
          ScoredVector(iVector = iVector, outClass = outClass, outScore = pSimilarity, iPoint = bestPoint, iCenter = iCenter)
        }
  }

  def affectPoint(vector:MLVector, tokens:Seq[String], vClass:Int, vScore:Double, iPoint:Int, iCenter:Int, weight:Double = 1.0, asVCenter:Option[MLVector]=None, fit:Boolean) {
    this.pScores(iPoint) = this.pScores(iPoint) + vScore * weight
    val vectorOrCenter =  asVCenter match {case Some(v) => v case _ => vector}

    if((this.sequences(iPoint).size != tokens.size 
          || It.range(0, tokens.size).map(i => String.CASE_INSENSITIVE_ORDER.compare(this.sequences(iPoint)(i),tokens(i)) == 0).contains(false)
        )  
        && !this.initializing 
        && fit
      )
      tryAsPoint(vector = vector, tokens = tokens, vClass = vClass, iPoint = iPoint, iCenter = iCenter)
    this.vCenters(iPoint) = this.vCenters(iPoint).scale(pScores(iPoint)/(pScores(iPoint) + weight)).sum(vectorOrCenter.scale(weight).scale(weight/(pScores(iPoint) + weight)))
    this.pGAP(iPoint) = 1.0 - this.vCenters(iPoint).similarityScore(this.points(iPoint))
    this.cError(iCenter) = this.cError(iCenter) * (cHits(iCenter)/(cHits(iCenter) + weight)) + (1.0 - vectorOrCenter.similarityScore(this.points(iPoint))) * (weight/(cHits(iCenter) + weight))
    this.cHits(iCenter) = this.cHits(iCenter) + weight
  }
  def tryAsPoint(vector:MLVector, tokens:Seq[String], vClass:Int, iPoint:Int, iCenter:Int) {
    val newGAP = 1.0 - this.vCenters(iPoint).similarityScore(vector)
    if(newGAP - this.pGAP(iPoint) < 0) {
      /*if(Seq(2, 3).contains(vClass)){
        println(s"gap: ${this.pGAP(iPoint)}> $newGAP replacing ${this.sequences(iPoint)} ${newGAP - this.pGAP(iPoint)} by ${tokens} ${this.points(iPoint).similarityScore(vector)}")
      }*/
      this.points(iPoint) = vector
      this.sequences(iPoint) = tokens
      this.updateParams(None, false)
    }
  }

  def GAP = {
    val allScores = this.pScores.sum
    It.range(0, this.pGAP.size)
      .map{iPoint => this.pGAP(iPoint)*(this.pScores(iPoint)/allScores)}
      .reduce(_ + _)
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
        if(fit & that.children.filter(c => c.params.hits> 0).size > 1) {
            that.children.filter(c => c.params.hits> 0).foreach(c => this.mergeWith(thatNode = c, cGenerator = cGenerator, fit = fit))
        } else if(fit) {
          val vectorsInScopeCount =  that.points.size
          val outToInClass = 
            this.links.keysIterator
              .flatMap(thisIn => that.links(thisIn).map(thatOut => (thatOut, thisIn)))
              .toSeq
              .groupBy{case (thatOut, thisIn) => thatOut}
              .mapValues{s => s.map{case (thatOut, thisIn) => thisIn}.head}
          
          (for(iCenter <- It.range(0, this.numCenters)) 
            yield {
              val scoresByClass = that.classCentersMap(iCenter).iterator
                .map{leafClass => 
                   (outToInClass(leafClass)
                     , that.rel.get(leafClass).map(pairs => pairs.keysIterator).getOrElse(It[Int]()).map{ iLeafPoint =>
                        val scoredPoint = 
                          this.score(
                            iVector = iLeafPoint
                            , vectors = that.points
                            , vTokens = that.sequences.map(t => t match {case Seq(t) => t case _ => throw new Exception("Multi token clustering not yet suported")})
                            , inClass = outToInClass(leafClass) 
                            , parent = None/*parent match {case Some(c) => c match {case c:ClusteringNode => Some(c) case _ => None} case _ => None}*/
                            , cGenerator = cGenerator
                            , fit = fit
                          )
                        scoredPoint
                       }
                   )
                }
              val sequenceScore = this.scoreSequence(scoredTokens = scoresByClass.map{case (inClass, scores) => (inClass, scores.flatMap(s => s))})
              sequenceScore.map{case ScoredSequence(inClass, outClass, score, scoredVectors) => 
                for(ScoredVector(iVector, outVectorClass, outVectorScore, iPoint, iCenter) <- scoredVectors ) {
                  this.params.hits = this.params.hits + that.params.hits / vectorsInScopeCount
                  this.affectPoint(
                    vector = that.points(iVector)
                    , tokens = that.sequences(iVector)
                    , vClass = outClass
                    , vScore = outVectorScore
                    , iPoint = iPoint
                    , iCenter = iCenter
                    , weight = that.params.hits / vectorsInScopeCount
                    , asVCenter = Some(this.vCenters(iCenter))
                    , fit = fit
                  )
                }
                (outClass, score)
              }.reduce((p1, p2) => (p1, p2) match{case((_, score1),(_, score2)) => if(score1>score2)  p1 else p2})
            }).reduce((p1, p2) => (p1, p2) match{case((_, score1),(_, score2)) => if(score1>score2)  p1 else p2})
             match {
               case (outClass, _) =>
                 for(i <- It.range(0, this.children.size)) {
                   if(this.children(i).params.filterValue.contains(outClass)) 
                     (this.children(i)).mergeWith(thatNode, cGenerator = cGenerator, fit = fit)
                 }
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
  def resetHitsExtras {
    It.range(0, this.pScores.size).foreach(i => pScores(i) = 0.0)
    It.range(0, this.cHits.size).foreach(i => cHits(i) = 0.0)
    It.range(0, this.pGAP.size).foreach(i => pGAP(i) = 0.0)
    It.range(0, this.cError.size).foreach(i => cError(i) = 0.0)
    It.range(0, this.pGAP.size).foreach(i => pGAP(i) = 0.0)
  }
  def cloneUnfittedExtras = this.params.cloneWith(classMapping = None, unFit = true).get.toNode().asInstanceOf[this.type]
  def updateParamsExtras {
    this.params.cError = Some(this.cError.clone)
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
    index match {
      case Some(ix) if ret.sequences.size > 0 =>
        ret.points ++= (ix(ret.sequences.flatMap(t => t).distinct) match {case map => ret.sequences.map(tts => tts.flatMap(token => map.get(token)).reduceOption(_.sum(_)).getOrElse(null))})  
      case _ =>
    }
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
    encoded.deserialize[ArrayBuffer[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("pGAP").zipWithIndex.foreach{case (v, i) => ret.pGAP(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cHits").zipWithIndex.foreach{case (v, i) => ret.cHits(i) = v}
    ret 
  }
}

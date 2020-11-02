package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap, ListBuffer}
import scala.{Iterator => It}
import java.sql.Timestamp

/** A node for clustering operations
 *
 * @param params @tparam NodeParams Summarizes several node parameters
 * @param points @tparam ArrayBuffer[MLVector]  Vectors for top words of this node
 * @param children @tparam ArrayBuffer[Node] Node children
 */
case class ClusteringNode (
  params:NodeParams
  , points: ArrayBuffer[MLVector] // vectors for topwords of this node
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  assert(!this.params.maxTopWords.isEmpty)
  assert(!this.params.classCenters.isEmpty)
  assert(!this.params.childSplitSize.isEmpty)
  val maxTopWords = this.params.maxTopWords.get
  val classCenters = HashMap(this.params.classCenters.get.map{case (classStr, center) => (classStr.toInt, center)}.toSeq :_*)
  val childSplitSize = this.params.childSplitSize.get
  val classCentersMap = classCenters.groupBy{case (cla, center) => center}.mapValues(p => p.map{case (cla, center) => cla}.toSet)

  // For each topword: average score of tokens to its topword
  val pScores = if(this.params.annotations.size == maxTopWords) Array(this.params.annotations.map(a => a.score) :_*) else Array.fill(maxTopWords)(0.0)
  val numCenters = this.classCenters.values.toSet.size
  val cError = this.params.cError.getOrElse(Array.fill(numCenters)(0.0)) // average distance/error of all tokens to its closest topword

  var initializing = this.points.size < this.maxTopWords
  lazy val vectorSize = 200//this.points(0).size
  lazy val vZero = Vectors.dense(Array.fill(vectorSize)(0.0))
  lazy val vCenters = ArrayBuffer.fill(maxTopWords)(vZero) // Array over topwords: Weighted average of all tokens having the highest score for the topword of a center
  var center = null.asInstanceOf[MLVector] // average vector of all tokens going through this node
  //lazy val classPointsSum = HashMap(this.params.classCenters.get.map{case (classStr, center) => (classStr.toInt, vZero)}.toSeq :_*) // Map [outClass, sum of vectors for each topwords]
  val pGAP = ArrayBuffer.fill(maxTopWords)(1.0) // distance of a token to its center for the specific topword
  val cHits = ArrayBuffer.fill(numCenters)(0.0) // number of documents going through one of the (two) child clusters


  def encodeExtras(encoder:EncodedNode) {
    //thiese two are not necessary if params are updated at encode time. Please remove after test
    encoder.serialized += (("pScores",serialize(pScores) ))
    encoder.serialized += (("cError",serialize(cError) ))
    encoder.serialized += (("initializing", serialize(initializing)))
    if(this.points.size > 0) encoder.serialized += (("vCenters",serialize(vCenters )))
    if(this.points.size > 0) encoder.serialized += (("center",serialize(center )))
    encoder.serialized += (("pGAP",serialize(pGAP )))
    encoder.serialized += (("cHits",serialize(cHits) ))
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> classCenters: ${classCenters}\n")
    buffer
  }

  def toTag(id:Int):TagSource = ClusterTagSource(
    id = this.params.tagId.getOrElse(id)
    , operation = TagOperation.create
    , timestamp = Some(new Timestamp(System.currentTimeMillis()))
    , name = Some(this.params.name)
    , color = this.params.color
    , strLinks = Some(this.params.strLinks)
    , maxTopWords = this.params.maxTopWords
    , childSplitSize = this.params.childSplitSize
    , oFilterMode = Some(this.params.filterMode)
    , oFilterValue = Some(this.params.filterValue.toSet)
  )

  /** A document runs through this node and the node parameters are updated
   *
   * @param facts @tparam HashMap[Int, HashMap[Int,Int]] Map(class, Map(token positions, per class )) : Map all tokens to the classes they are associated to
   * @param scores @tparam HashMap[Int, Double] Score for each class that has already been evaluated
   * @param vectors @tparam Seq[MLVector] Vectors for the tokens
   * @param parent @tparam Option[Node] Parent Node
   * @param cGenerator @tparam Iterator[Int] Generates new classes for children when new ones are created
   * @param fit @tparam Boolean Should model/topwords be updated for the new documents
    */
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
      , scores:HashMap[Int, Double]
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGenerator:Iterator[Int]
      , fit:Boolean) {

    val vectorsInScopeCount =  this.links.keysIterator.map(inClass => facts.get(inClass).map(o => o.size).getOrElse(0)).sum // number tokens in scope
    //println(s"numCenters: $numCenters; classCenters : $classCenters; classCentersMap: $classCentersMap")
    //println("facts:")
    //println(facts)
    //println("tokens:"+tokens.zipWithIndex.mkString(";"))

    // for each token calculate a score for each possible child class (two scores for each token
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
                //val (scoredPoint1, scoredPoint2) = scoredPoint.duplicate
                //println(s"\t1 inClass $inClass, iVector: $iBase, tokens($iBase): ${tokens(iBase)}")
                //println(s"\t1 scoredPoint:")
                //scoredPoint1.foreach(println)
                //scoredPoint2
                scoredPoint
              }
          )
        }

    // calculate global score for each of the two possible child classes => choose highest score which is class for this document
    val sequenceScore = this.scoreSequence(scoredTokens = scoresByClass.map{case (inClass, scores) => (inClass, scores.flatMap(s => s))  })//.toSeq // sequence = documents

    sequenceScore.map{case ScoredSequence(inClass, outClass, score, scoredVectorsIt) =>
      val scoredVectors = scoredVectorsIt.toSeq
      scores(outClass) = score
      val vectorsInClassCount = scoredVectors.filter(_.hasHighestScore).size


      // it may happen that the global document score 'score' is for one outClass, but all its single tokens scored highest for the other outClass
      // => take token with highest score as vectorInClass
      val vectorsInClass = {
        if (vectorsInClassCount > 0)
        scoredVectors.filter(_.hasHighestScore)
        else // no token scored highest for the outClass with highest global score
          Seq(scoredVectors.sortWith(_.outScore > _.outScore).take(1)) // takes first element
      }
      //val vectorsInClass = scoredVectors

      for(ScoredVector(iVector, outVectorClass, outVectorScore, iPoint, iCenter, hasHighestScore) <- vectorsInClass ) {
        //println(s"\t outClass: $outClass, outVectorClass: $outVectorClass, outVectorScore: $outVectorScore, iVector: $iVector, iPoint: $iPoint, iCenter: $iCenter, tokens: ${Seq(tokens(iVector)).mkString("; ")}")

        //affects this class outClass to this vector ; all tokens of this sentence have the class outClass
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
          , weight = 1.0 / vectorsInClass.size//vectorsInClassCount
          , asVCenter = None
          , fit = fit
        )

        // facts.get(outClass) match {
        //   case Some(f) => f(iVector) = iVector
        //   case None => facts(outClass) = HashMap(iVector -> iVector)
        // }
        //
        // this.affectPoint(
        //   vector = vectors(iVector)
        //   , tokens = Seq(tokens(iVector))
        //   , vClass = outClass
        //   , vScore = outVectorScore
        //   , iPoint = iPoint
        //   , iCenter = iCenter
        //   , weight = 1.0 / vectorsInScopeCount
        //   , asVCenter = None
        //   , fit = fit
        // )

      }
    }.size

  }

  /** Affects top word for initialisation of a new node
   *
   * @param vector @tparam MLVector Vector for token
   * @param tokens @tparam Seq[String] Token (or sequence of tokens) to be affected as new top word
   * @param inClass @tparam Int Specifies the node to whose child the token is affected to
   */
  def onInit(vector:MLVector, tokens:Seq[String], inClass:Int) = {
    //if(this.params.hits < 10) //println(s"${this.children.size} ${this.classCentersMap.size} ${this.initializing} ${this.pScores.sum} ${this.childSplitSize} ${this.points.size} < ${this.maxTopWords} ${this.params.filterValue} ${inClass}")
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
             //this.classPointsSum(classToFill) = this.rel(classToFill).keysIterator.map(i => this.points(i)).reduce(_.sum(_)) // center of topwords
             initializing = this.points.size < this.maxTopWords
             classToFill
         }
       //println(s"init pClasses: ${this.pClasses.toSeq}")
    }
      //println(It.range(1, 3).map(t => s"($t) ${this.classPath.filter{case(cat, parents) => parents(t)}.map{case (cat, _) => this.rel.get(cat) match { case Some(values) => values.map{case (iClass, _) => this.tokens(iClass)}.mkString(",") case None => "-" }}} ").mkString("<---->"))
    else
      -1
  }

  /** Average of how close (1+cos sim/2) tokens are from their topwords */
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

  /** Score of document for its best child class
   *
   * @param inClass @tparam Int Specifies the node from which the document enters
   * @param outClass @tparam Int Specifies the child node with highest score to which the document goes
   * @param outScore @tparam outScore Highest score for a document to go to one of its childs
   * @param scoredVectors @tparam Iterator[ScoredVector] All tokens with scores that are clustered in the same child class than outClass
   */
  case class ScoredSequence(
    inClass:Int
    , outClass:Int
    , outScore:Double
    , scoredVectors:Iterator[ScoredVector])

  /** Calculate global score for a document for each of the (two) child classes
   *
   * @param scoredTokens @tparams Iterator[(Int, Iterator[ScoredVector])] For each inClass the associated scores for its tokens (ScoredVector)
   * @return ScoredSequence for the the outClass with higher score
   */
  def scoreSequence(scoredTokens:Iterator[(Int, Iterator[ScoredVector])]) = {
    val pointScores = Array.fill(this.maxTopWords)(0.0) // calculates mean of scores of token to topwords
    val pointCounts = Array.fill(this.maxTopWords)(0) // how many tokens will be assigned to this topword
    val sequenceScore = // returns two scoreSequence, which contain a particular class
     for((inClass, inClassScores)  <- scoredTokens)
        yield {
          val retTokens = this.outClasses.map(o => (o, ListBuffer[ScoredVector]())).toMap
          val (bestClass, bestScore) = {
            for(scoredToken  <- inClassScores) { // sums for each topword the scores
              pointScores(scoredToken.iPoint) = pointScores(scoredToken.iPoint) + scoredToken.outScore // iPoint = index of the topword
              pointCounts(scoredToken.iPoint) = pointCounts(scoredToken.iPoint) + 1
              retTokens(scoredToken.outClass) += scoredToken
              //println(s"\t\t  iVector: ${scoredToken.iVector} ,pointScores(${scoredToken.iPoint}): ${pointScores(scoredToken.iPoint)}; pointCounts(scoredToken.iPoint): ${pointCounts(scoredToken.iPoint)}; retTokens(${scoredToken.outClass}): ${retTokens(scoredToken.outClass)}")
            }

            this.links(inClass).iterator
              .map{outClass =>
                var vectorCount = 0
                val pointScore = // score for each topword for this outClass
                  this.rel.get(outClass).map(_.keysIterator).getOrElse(Iterator[Int]())
                    .map{iPoint =>
                      val pointScore = if(pointCounts(iPoint) == 0) 0.0 else pointScores(iPoint)/pointCounts(iPoint)
                      vectorCount = vectorCount + 1
                      pointScore
                    }
                    .reduceOption(_ + _).getOrElse(0.0)
                var outClassScore = (outClass, if(pointScore == 0) 0.0 else pointScore / vectorCount)
                //println(s"\t\toutClassScore: $outClassScore, outClass: $outClass")
                outClassScore
              }
              .reduce((p1, p2) => (p1, p2) match {case ((_, score1),(_, score2)) => if(score1 > score2) p1 else p2}) // take best score of both possible outClasses
          }
          ScoredSequence(inClass = inClass, outClass = bestClass, outScore = bestScore, scoredVectors = retTokens(bestClass).iterator) // scoredVectors : score for each token
        }
    sequenceScore
  }

  /** Similarity score for a token to a topword to one of its (two) child classes
   *
   * @param iVector @tparam Int Index of token
   * @param outClass @tparam Int The child class the token is compared with
   * @param outScore @tparam Do uble Similarity score of token to the topword in the child class
   * @param iPoint @tparam Intndex of topword
   * @param iCenter @tparam Int Index of the center (of this topword)
   * @param hasHighestScore @tparam Boolean If outScore is the highest of (both) similarity scores for the token to its childs
   */
  case class ScoredVector(
    iVector:Int
      , outClass:Int
      , outScore:Double
      , iPoint:Int
      , iCenter:Int
      , hasHighestScore:Boolean=true)

  /** Evaluates model and fits token to one of the childs by calculating a similarity score
   *
   * @param iVector @tparam Int Index of token and vector
   * @param vectors @tparam Seq[MLVector] Vectors for the tokens
   * @param vTokens @tparam Seq[String] Token (or sequence of tokens)
   * @param inClass @tparam Int The class from which the token is coming
   * @param parent @tparam Option[ClusteringNode] Parent Node
   * @param cGenerator @tparam Iterator[Int] Generates new classes for children when new ones are created
   * @param fit @tparam Boolean Should model/topwords be updated for the new documents
   *
   * @return ScoredVector Similarity score of token to a topword of the child nodes
   */
  def score(
    iVector:Int
    , vectors:Seq[MLVector]
    , vTokens:Seq[String]
    , inClass:Int
    , parent:Option[ClusteringNode]=None
    , cGenerator: Iterator[Int]
    , fit:Boolean) = {

    //val vectorOrCenter = asVCenter match {case Some(v) => v case None => vectors(iVector) }
    // create new clustering children
    //println(s"\ntokens: ${vTokens.mkString(" ")}")
    if (this.params.filterValue == 4) {
      println(s"childSplitSize: ${this.params.childSplitSize}, strLinks: ${this.params.strLinks}")
      println(s"fit: $fit, !initializing:${!this.initializing} this.children.size: ${this.children.size}")
      println(s"pScores.sum: ${this.pScores.sum}; this.childSplitSize: ${this.childSplitSize}")

    }
    if(fit
        && cGenerator.hasNext // create children until limit is reached : maxClasses
        && this.children.size < this.classCentersMap.size // create children only if children do not exist already
        && !this.initializing
        && this.pScores.sum > this.childSplitSize // pScores: score for each point/topword ; sum(pScores) == number of documents went through this node
        && (parent.isEmpty || parent.get.cHits.forall(_ > this.childSplitSize))
        //&& (parent.isEmpty || this.clusterScore < 0.9) /*(this.clusterScore - parent.get.clusterScore)/parent.get.clusterScore > 0.02 )*/
      ) {
      //println(s"\tspawning... ${this.params.annotations}, ${this.inClasses}")
      this.fillChildren(cGenerator) // create children
    }
    //else if (fit && this.initializing) { // see 3,4 prints  // && this.params.filterValue ==
    //else if (fit && !this.initializing && this.pScores.sum < this.childSplitSize && this.params.filterValue(0) == 3) { // ~ 50 prints per filterValue : pScores.sum = 50
    //else if (fit && !this.initializing && this.pScores.sum < this.childSplitSize && this.params.filterValue(0) == 2) { // ~ 50 prints per filterValue : pScores.sum = 13
    //  else if (fit && !this.initializing && this.pScores.sum > this.childSplitSize && (parent.isEmpty || !parent.get.cHits.forall(_ > this.childSplitSize))) { // ~ 50 prints per filterValue : pScores.sum = 13
    //    println(s"\niVector: $iVector; inClass: $inClass, vTokens($iVector): ${vTokens(iVector)}")
    //    println(s"\tfilterValue: ${this.params.filterValue(0)}")
    //    println(s"\tfit: $fit,  !this.initializing: ${!this.initializing}")
    //    println(s"\tthis.children.size (${this.children.size}) < this.classCentersMap.size (${this.classCentersMap.size}): ${this.children.size < this.classCentersMap.size}")
    //    println(s"\tthis.pScores.sum (${this.pScores.sum}) > this.childSplitSize (${this.childSplitSize}) : ${this.pScores.sum > this.childSplitSize}")
    //    println(s"\tparent.isEmpty : ${parent.isEmpty}")
    //    if (!parent.isEmpty) {
    //      println(s"\tparent.get.cHits.forall(_ > this.childSplitSize): ${parent.get.cHits.forall(_ > this.childSplitSize)}")
    //      parent.get.cHits.foreach(hit => if (hit < this.childSplitSize) println(s"\t hit (${hit}) < childSplitSize (${this.childSplitSize})"))
    //    }
    // // //
    // }
    onInit(vectors(iVector), Seq(vTokens(iVector)), inClass) // affecting top words for initalisation if not initialised
    this.links(inClass).iterator
      .filter{outClass => this.rel.contains(outClass)}
      .map{outClass =>
        val iCenter = this.classCenters(outClass)
        //println(s"\t\t outClass: $outClass, iCenter: $iCenter, this.rel(outClass): ${this.rel(outClass)}, this.classCenters: ${this.classCenters}")
        val (bestPoint, pSimilarity) =
           (for(iPoint <- this.rel(outClass).keysIterator) yield {
             val simScore = vectors(iVector).similarityScore(this.points(iPoint))
             //println(s"\t\t  iCenter: $iCenter, simScore: $simScore, vTokens($iVector): ${vTokens(iVector)}, topwords: ${this.sequences(iPoint)}")
             (iPoint, simScore)
             //yield (iPoint, vectors(iVector).similarityScore(this.points(iPoint)))
           }
           )
            .reduce((p1, p2) => (p1, p2) match {
              case ((i1, score1), (i2, score2)) =>
                if(score1 > score2) p1
                else p2
            })
          //println(s"\t\t outClass: $outClass, outScore: $pSimilarity, bestPoint: $bestPoint, iCenter: $iCenter, token($iVector): ${vTokens(iVector)}")
          (iVector, outClass, pSimilarity, bestPoint, iCenter)
          //ScoredVector(iVector = iVector, outClass = outClass, outScore = pSimilarity, iPoint = bestPoint, iCenter = iCenter) // evaluates current vector on each cluster class, returns two scoredvectors
      }
      .toList.sortBy(_._3)(Ordering[Double].reverse) // third argument is similarity score; sort token wiht highest score to the top of the list
      .zipWithIndex
      .map{case ((iVector, outClass, pSimilarity, bestPoint, iCenter), i) =>
        if (i == 0) // only scored vector with highest similarity score has the parameter 'hasHighestScore' set to true
          ScoredVector(iVector = iVector, outClass = outClass, outScore = pSimilarity, iPoint = bestPoint, iCenter = iCenter, hasHighestScore=true)
        else
          ScoredVector(iVector = iVector, outClass = outClass, outScore = pSimilarity, iPoint = bestPoint, iCenter = iCenter, hasHighestScore=false)
      }
      .iterator
  }

  /** Affects the class to a token of this sentence
   *
   * @param vector @tparam MLVector Vector for the token
   * @param Tokens @tparam Seq[String] Token (or sequence of tokens)
   * @param vClass @tparam Int The class to be affected to the token
   * @param vScore @tparam Double The similarity score of the token to the topword of this class
   * @param iPoint @tparam Int Index of the topword
   * @param iCenter @tparam Int Index of the child center
   * @param weight @tparam Double 1.0 / number of tokens going through this node
   * @param asVCenter @tparam Option[MLVector]
   * @param fit @tparam Boolean Should model/topwords be updated for the new documents
   */
  def affectPoint(
    vector:MLVector
    , tokens:Seq[String]
    , vClass:Int
    , vScore:Double
    , iPoint:Int
    , iCenter:Int
    , weight:Double = 1.0
    , asVCenter:Option[MLVector]=None
    , fit:Boolean) {
    //println(s"\t\t vClass: ${vClass}, vScore: ${vScore}, iPoint: ${iPoint}, iCenter: $iCenter, weight: $weight, tokens: ${tokens.mkString(";")}")
    this.pScores(iPoint) = this.pScores(iPoint) + vScore * weight
    val vectorOrCenter =  asVCenter match {case Some(v) => v case _ => vector}
    // if not initializing
    if((this.sequences(iPoint).size != tokens.size
          || It.range(0, tokens.size).map(i => String.CASE_INSENSITIVE_ORDER.compare(this.sequences(iPoint)(i),tokens(i)) == 0).contains(false)
        )
        && !this.initializing
        && fit
      )
      tryAsPoint(vector = vector, tokens = tokens, vClass = vClass, iPoint = iPoint, iCenter = iCenter) // tries this point as a topword and checks if there is an improvement

    //println(s"\t\t pScores(iPoint)/(pScores(iPoint) + weight): ${pScores(iPoint)/(pScores(iPoint) + weight)}, weight/(pScores(iPoint) + weight): ${weight/(pScores(iPoint) + weight)}")
    this.vCenters(iPoint) = this.vCenters(iPoint).scale(pScores(iPoint)/(pScores(iPoint) + weight)).sum(vectorOrCenter.scale(weight/(pScores(iPoint) + weight))) // update center/statistic for each topword
    this.center = It.range(0, this.pScores.size).map(iPoint => this.vCenters(iPoint).scale(pScores(iPoint))).reduce(_.sum(_)).scale(1.0/this.pScores.sum)
    this.pGAP(iPoint) = 1.0 - this.vCenters(iPoint).similarityScore(this.points(iPoint))
    //println(s"pGAP: ${this.pGAP(iPoint)}, iPoint: ${iPoint}, pScores: ${pScores(iPoint)}")
    // cError: average distance/error of all tokens to its closest topword
    this.cError(iCenter) = this.cError(iCenter) * (cHits(iCenter)/(cHits(iCenter) + weight)) + (1.0 - vectorOrCenter.similarityScore(this.points(iPoint))) * (weight/(cHits(iCenter) + weight))
    this.cHits(iCenter) = this.cHits(iCenter) + weight
  }

  /** Tries a token as a new topword and checks if there is an improvement
   *
   * @param vector @tparam MLVector Vector of token
   * @param tokens @tparam Seq[String] Token (or several tokens) to be tried as new topword
   * @param vClass @tparam Int Class of the node with this topword
   * @param iPoint @tparam Int Index of topword
   * @param iCenter @tparam Int Index of center in which the topword is clustered
   */
  def tryAsPoint(vector:MLVector, tokens:Seq[String], vClass:Int, iPoint:Int, iCenter:Int) {
    // option 0
    //val newGAP = 1.0 - this.vCenters(iPoint).similarityScore(vector)
    //if(newGAP - this.pGAP(iPoint) < 0) {

    // option 1: min distance
    // val newGAP = 1.0 - this.vCenters(iPoint).similarityScore(vector)
    // val centerSimilarity = this.center.similarityScore(vector)
    // if(newGAP - this.pGAP(iPoint) < 0 && centerSimilarity < 0.8) {

    // option 2: instead of comparing gap to single topword, compare to all topwords
    // val classPointsSum = this.rel(vClass).keysIterator.map(i => this.points(i)).reduce(_.sum(_)) // center of topwords
    // val classCenterSum = this.rel(vClass).keysIterator.map(i => this.vCenters(i)).reduce(_.sum(_))
    // val newPointSum =  this.rel(vClass).keysIterator.map(i => if (i == iPoint) vector else this.points(i)).reduce(_.sum(_))
    // val newGAP = 1.0 - newPointSum.similarityScore(classCenterSum)
    // val currentGap = 1.0 - classPointsSum.similarityScore(classCenterSum)
    // if(newGAP < currentGap) {

    // option 3 : Force topword to be more different, not too close to existing topwords
    // tooClose: if topword candidate is more similar to all documents (true) or more similar to center of topwords (false)
    val newGAP = 1.0 - this.vCenters(iPoint).similarityScore(vector)
    val classPointsSum = this.rel(vClass).keysIterator.map(i => this.points(i)).reduce(_.sum(_)) // center of topwords
    val tooClose = this.center.similarityScore(vector) > classPointsSum.similarityScore(vector)    //val tooClose = this.center.similarityScore(vector) > this.classPointsSum(vClass).similarityScore(vector)
    if(newGAP - this.pGAP(iPoint) < 0 && !tooClose) {

      /*if(Seq(2, 3).contains(vClass)){
        println(s"gap: ${this.pGAP(iPoint)}> $newGAP replacing ${this.sequences(iPoint)} ${newGAP - this.pGAP(iPoint)} by ${tokens} ${this.points(iPoint).similarityScore(vector)}")
      }*/
      //this.classPointsSum(vClass) = this.rel(vClass).keysIterator.map(i => this.points(i)).reduce(_.sum(_)) // center of topwords
      this.points(iPoint) = vector
      this.sequences(iPoint) = tokens
      this.updateParams(None, false)
    }
  }

  def GAP = { // iPoint = top words
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
        if(fit & that.children.filter(c => c.params.hits> 0).size > 1) { // you walk until non-empty leaf nodes
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
                        val scoredPoints =
                          this.score(
                            iVector = iLeafPoint
                            , vectors = that.points
                            , vTokens = that.sequences.map(t => t match {case Seq(t) => t case _ => throw new Exception("Multi token clustering not yet supported")})
                            , inClass = outToInClass(leafClass)
                            , parent = None/*parent match {case Some(c) => c match {case c:ClusteringNode => Some(c) case _ => None} case _ => None}*/
                            , cGenerator = cGenerator
                            , fit = fit
                          )
                        scoredPoints //score of one the 3 topwords of this to the topwords of that
                       }
                   )
                }

              // sequence of one element, with the class of that center and contains a sequence of scoredPoints (= array of the two possible classes for this node)
              val sequenceScore = this.scoreSequence(scoredTokens = scoresByClass.map{case (inClass, scores) => (inClass, scores.flatMap(s => s))}) // flatmap: makes of List[lists] a list
              sequenceScore.map{case ScoredSequence(inClass, outClass, score, scoredVectors) =>
                for(ScoredVector(iVector, outVectorClass, outVectorScore, iPoint, iCenter, hasHighestScore) <- scoredVectors ) {
                  this.params.hits = this.params.hits + that.params.hits / vectorsInScopeCount
//                  TODO: merge externalClassesFreq for this that
//                  TODO: check if division
                  // recalculates centers of this based on that and checks if topwords are better
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
//          TODO: merge externalClassesFreq for this that
//          TODO: calculate purity based on externalClassesFreq
//          this.params.purity = ..
          It.range(0, this.pScores.size).foreach(i => this.pScores(i) =  this.pScores(i) + that.pScores(i) )
          It.range(0, this.cError.size).foreach(i => this.cError(i) = this.cError(i) * (this.cHits(i)/(this.cHits(i) + that.cHits(i))) + that.cError(i) * (that.cHits(i)/(that.cHits(i) + this.cHits(i))))
          It.range(0, this.cHits.size).foreach(i => this.cHits(i) =  this.cHits(i) + that.cHits(i))
          if (this.points.size > 0) {
            if (this.pGAP.size != this.points.size) println(s"this.pGAP.size: ${this.pGAP.size}, this.points.size: ${this.points.size}")
            It.range(0, this.vCenters.size).foreach(i => this.vCenters(i) = this.vCenters(i).scale(this.pScores(i)/(this.pScores(i) + that.pScores(i))).sum(that.vCenters(i).scale(that.pScores(i)/(that.pScores(i) + this.pScores(i)))))
            It.range(0, this.points.size).foreach(i => this.pGAP(i) = 1.0 - this.vCenters(i).similarityScore(this.points(i)))
          }
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
    // creates two empty children for current node and affects classes and parameters to collect sentences as expected
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
    if(this.children.size < this.classCenters.values.toSet.size) {
      this.children.clear
    }
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
    if(encoded.points.size > 0) encoded.deserialize[ArrayBuffer[MLVector]]("vCenters").zipWithIndex.foreach{case (v, i) => ret.vCenters(i) = v}
    if(encoded.points.size > 0) ret.center = encoded.deserialize[MLVector]("center")
    encoded.deserialize[ArrayBuffer[Double]]("pGAP").zipWithIndex.foreach{case (v, i) => ret.pGAP(i) = v}
    encoded.deserialize[ArrayBuffer[Double]]("cHits").zipWithIndex.foreach{case (v, i) => ret.cHits(i) = v}
    ret
  }
}

package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.mllib.linalg.implicits._
import demy.util.{log => l}
import demy.mllib.tuning.BinaryOptimalEvaluator
import demy.mllib.evaluation.BinaryMetrics
import org.apache.spark.ml.linalg.{Vector => MLVector}
import org.apache.spark.sql.{SparkSession}
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import scala.{Iterator => It}
import java.sql.Timestamp
import scala.util.Random

/** A ClassifierNode to train a classifier from annotations
 *
 * @param points The word vectors for the annotations (Training set)
 * @param params @tparam NodeParams The parameters of the node
 * @param children @tparam ArrayBuffer[Node] Array of children nodes for this Classifier node
 */
case class ClassifierNode (
  points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]() // fastText vector
  , params:NodeParams
  , children: ArrayBuffer[Node] = ArrayBuffer[Node]()
) extends Node {
  val models = HashMap[Int, WrappedClassifier]()
  val windowSize = this.params.windowSize.getOrElse(2)
  def encodeExtras(encoder:EncodedNode) {
    encoder.serialized += (("models", serialize(models.map(p => (p._1, p._2.model)))))
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer
  }
  /** Create ClassifierTagSource from TagSource */
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
    , windowSize = Some(this.windowSize)
  )

  /** Transform updates the facts and scores
   *
   * @param facts @tparam HashMap[Int, HashMap[Int, Int]] Mapping for each class of vectors a HashMap with the indices of vectors having the class
   * @param scores @tparam HashMap[Int, Double] Maps for each class the global score of all vectors having this class
   * @param vectors @tparam Seq[MLVector] List of word vectors
   * @param tokens @tparam Seq[String] List of tokens
   * @param parent @tparam Option[Node] Parent node
   * @param cGenerator @tparam Iterator[Int]
   * @param fit @tparam Boolean
  */
  def transform(facts:HashMap[Int, HashMap[Int, Int]] // (class of vectors, HashMap(Indices of vectors having the class , _unimportant_))
      , scores:HashMap[Int, Double] // Int : class; Double : global score for all vectors in this class
      , vectors:Seq[MLVector]
      , tokens:Seq[String]
      , parent:Option[Node]
      , cGenerator:Iterator[Int]
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
            }.takeWhile{case (i, score) => i < iPos + windowSize || i < bestTo + windowSize}
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

  /** Returns fitted Classifier Node
   *
   * @param spark @tparam SparkSession
   * @param excludedNodes @tparam Seq[Node]
   * @return Fitted Classifier Node
  */
  def fit(spark:SparkSession, excludedNodes:Seq[Node]) = {
    l.msg(s"Start classifier fitting models for windowSize ${this.windowSize}")
    this.models.clear
    val thisPoints = this.points.filter(_ != null)
    val thisClasses = (c:Int) =>
      (for(i<-It.range(0, this.points.size))
        yield(this.rel(c).get(i) match {
          case Some(from) if this.inRel(c)((i, from)) => c
          case _ => -1
        })
      ).toSeq
       .zip(this.points)
       .flatMap{case(c, p) => if(p == null) None else Some(c)}
    def getPoints(nodes:Seq[Node], positive:Boolean, negative:Boolean):Iterator[(MLVector, Boolean)] =
      if(nodes.isEmpty) Iterator[(MLVector, Boolean)]()
      else (
        nodes
          .iterator.flatMap{case n:ClassifierNode => Some(n) case _ => None}
          .flatMap(n =>
            n.inRel.values.iterator
              .flatMap(points => points.iterator
                .flatMap{case ((i, from), inRel) =>
                  if(inRel && positive) Some(n.points(i), true)
                  else if (!inRel && negative) Some(n.points(i), false)
                  else None
           })) ++ getPoints(nodes.flatMap(n => n.children.flatMap{case c:ClassifierNode => Some(c) case _ => None}), positive, negative)
      )

    val otherPointsOut = getPoints(excludedNodes, true, false).map{case (v, inRel) => (v)}.toSeq
    val otherChildrenPoints = Iterator[(MLVector, Boolean)]() //getPoints(this.children, true, true).toSeq
    for(c <- this.outClasses) {
      this.models(c) =
        WrappedClassifier(
          forClass = c
          , points = thisPoints ++ otherPointsOut ++ otherChildrenPoints.map(_._1)
          , pClasses = thisClasses(c) ++ otherPointsOut.map(_ => -1) ++ otherChildrenPoints.map{case (v, inRel) => if(inRel) c else -1}
          , spark = spark)
    }
    l.msg("Classifier fit")
    this
  }

  /** Returns metrics for classifier performance
   *
   * @param index @tparam Option[VectorIndex]
   * @param allAnnotations @tparam Seq[AnnotationSource] List of annotations
   * @param spark @tparam SparkSession
   * @return Tuple (metrics, node name, node tag id, classes, current time stamp, postive annotations, negative annotations)
  */
  def evaluateMetrics(index:Option[VectorIndex], allAnnotations:Seq[AnnotationSource], spark:SparkSession, excludedNodes:Seq[Node] = Seq[Node]()) : ArrayBuffer[PerformanceReport] = {
      import spark.implicits._
      val r = new Random(0)
      val currentAnnotations = allAnnotations.filter(a => this.outClasses(a.tag))
      val trainSet = r.shuffle(currentAnnotations).take((currentAnnotations.length*0.8).toInt).toSet
      val test = (currentAnnotations.toSet -- trainSet).toList
      val train = trainSet.toList
      val newParams = this.params.cloneWith(None, true) match {
        case Some(value) => value
        case None => throw new Exception("ERROR: cloneWith current classifier node returned None in function evaluateMetrics")
      }
      newParams.annotations ++= train.map(_.toAnnotation)
      val trainNode = newParams.toNode(vectorIndex = index).asInstanceOf[ClassifierNode]
      trainNode.fit(spark, excludedNodes)
      var output:ArrayBuffer[PerformanceReport] = ArrayBuffer.empty[PerformanceReport]
      for(c <- this.outClasses) {
        val testDF = test.map { a =>
          val tokens = a.tokens
          val vectors:Seq[MLVector] = index match {
            case Some(wordVectorIndex) => wordVectorIndex(tokens) match {
              case vectorsMap => tokens.map( (token:String) => vectorsMap.get(token).getOrElse(null))
            }
            case None => throw new Exception("Provided vectorIndex is None!")
          }
          val facts:HashMap[Int,HashMap[Int,Int]] = HashMap((this.inMap(c), HashMap(It.range(0, tokens.size).filter(i => vectors(i)!=null).map(i => i -> i).toSeq :_* )))
          val scores = HashMap[Int, Double]()
          trainNode.transform(facts = facts
              , scores = scores
              , vectors = vectors
              , tokens = tokens
              , parent = None
              , cGenerator = Iterator[Int]()
              , fit = false)
          (if(a.inRel) 1.0 else 0.0, scores.get(c).getOrElse(0.0))
        }.toDS()
         .withColumnRenamed("_1", "label")
         .withColumnRenamed("_2", "score")

        val evaluator = new BinaryOptimalEvaluator().fit(testDF)
        val metrics = evaluator.metrics
        this.params.metrics =
          this.params.metrics ++ Map(s"prec${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.precision.getOrElse(0.0),
                                  s"recall${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.recall.getOrElse(0.0),
                                  s"f1${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.f1Score.getOrElse(0.0),
                                  s"AUC${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.areaUnderROC.getOrElse(0.0),
                                  s"threshold${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.threshold.getOrElse(0.0)
                                )
        // TODO: check how to evaluate on multiclass case, return macro, micro, weighted measures ?
        output += PerformanceReport(metrics.threshold
                                  , metrics.precision
                                  , metrics.recall
                                  , metrics.f1Score
                                  , metrics.areaUnderROC
                                  , this.params.name
                                  , this.params.tagId
                                  , c
                                  , new Timestamp(System.currentTimeMillis())
                                  , (train++test.filter(a => a.inRel == true)).size
                                  , (train++test.filter(a => a.inRel == false)).size)

      }
      output
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

case class PerformanceReport(
    threshold:Option[Double]=None
  , precision:Option[Double]=None
  , recall:Option[Double]=None
  , f1Score:Option[Double]=None
  , areaUnderROC:Option[Double]=None
  , nodeName:String
  , tagId: Option[Int]
  , classId: Int
  , timestamp:Timestamp
  , positiveAnnotations: Int
  , negativeAnnotations: Int
)

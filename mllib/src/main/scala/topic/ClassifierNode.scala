package demy.mllib.topic

import demy.mllib.index.VectorIndex
import demy.mllib.linalg.implicits._
import demy.util.{log => l}
import demy.mllib.tuning.BinaryOptimalEvaluator
import demy.mllib.evaluation.BinaryMetrics
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
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
 * @param models @tparam HashMap[Int, WrappedClassifier] Map of classes to their Classifiers
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
        Some(this.score(outClass, vectors(iIn))) // each vector validated on classifiers
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
            .map{case (vSum, i) => (i, this.score(outClass, vSum), vSum)} // calls classifiers
            .map{case (i, score, vSum) =>
              if(score > bestPosScore) {
                bestPosScore = score
                bestTo = i
                bestITo = idx(i)
                bestSum = Some(vSum)
              }
              (i, score) // score of expanded window
            }.takeWhile{case (i, score) => i < iPos + windowSize || i < bestTo + windowSize} //
            .size // just to execute
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
      if (windowSize > 0)
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

  def getPoints(nodes:Seq[Node], positive:Boolean, negative:Boolean):Iterator[(MLVector, Boolean)] =
    if(nodes.isEmpty) Iterator[(MLVector, Boolean)]()
    else (
      nodes
        .iterator.flatMap{case n:ClassifierNode => Some(n)
                          case _ => None}
        .flatMap(n => {
          n.inRel.values.iterator
            .flatMap(points => points.iterator
              .flatMap{case ((i, from), inRel) => {
                if(inRel && positive && n.windowSize == this.windowSize) Some(n.points(i), true)
                else if (!inRel && negative && n.windowSize == this.windowSize) Some(n.points(i), false)
                else None
              }
         })
       }
     ) ++ getPoints(nodes.flatMap(n => {
                  n.children.flatMap{case c:ClassifierNode => Some(c)
                                     case _ => None}
                                  }), positive, negative)
    )

    def getDescendantClasses(nodes:Seq[Node]):Iterator[Int] = (
      if(nodes.isEmpty) Iterator[Int]()
      else (
        nodes
          .iterator.flatMap{case n:ClassifierNode => Some(n)
                            case _ => None}
          .flatMap(n => {
                  if(n.windowSize == this.windowSize) n.outClasses.iterator
                  else Iterator[Int]()
                }
           ) ++ getDescendantClasses(nodes.flatMap(n => {
                    n.children.flatMap{case c:ClassifierNode => Some(c)
                                       case _ => None}
                                    }))
      ))


  /** Returns fitted Classifier Node
   *
   * @param spark @tparam SparkSession
   * @param excludedNodes @tparam Seq[Node]
   * @return Fitted Classifier Node
  */
  def fit(spark:SparkSession, excludedNodes:Seq[Node]) = {
//  def fit(spark:SparkSession, excludedNodes:Seq[Node]) = {
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


    val otherPointsOut = getPoints(excludedNodes, true, false).map{case (v, inRel) => (v)}.toSeq
    val otherChildrenPoints = getPoints(this.children, true, false).toSeq
    println("name:"+this.params.name)
    println("size annotations: "+this.params.annotations.size)

    for(c <- this.outClasses) {
      this.models(c) = WrappedClassifier(
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
      val posClasses = getDescendantClasses(Seq(this)).toSet // get all classes for current and children node
      val negClasses = getDescendantClasses(excludedNodes).toSet // get all classes for brothers
      val currentAnnotationsPositive = allAnnotations.filter(a => posClasses(a.tag) && a.inRel)
      val currentAnnotationsNegative = allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel) || (negClasses(a.tag)  && a.inRel)).map(a => a.setInRel(false))
      // println("")
      // println("This Node: "+this.params.name)
      // println("Class: "+this.outClasses)
      // println("Excluded nodes:"+excludedNodes.size)
      // println("posClasses:")
      // println(posClasses)
      // println("negClasses:")
      // println(negClasses)
      // println("Positive current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && a.inRel)).size)
      // println("negative current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel)).size)
      // allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel)).foreach(a => if(a.tokens.size < 5) println("inRel:"+a.inRel+";;"+a.tokens) else println("tag: "+a.tag+"; inRel:"+a.inRel+";;"+a.tokens.take(8)))
      // println("currentAnnotationsPositive:"+currentAnnotationsPositive.size)
      // currentAnnotationsPositive.foreach(a => if(a.tokens.size < 5) println("tag: "+a.tag+"; inRel:"+a.inRel+";;"+a.tokens) else println("tag: "+a.tag+"; inRel:"+a.inRel+";;"+a.tokens.take(8)))
      val trainPos = if (currentAnnotationsPositive.length == 1) currentAnnotationsPositive.toSet
                     else {
                       // make sure that at least one positive training annotation from the current class (!) is in trainPos, otherwise the current class is not found in trainNode.rel
                       val annotCurrentClass = currentAnnotationsPositive.filter(a => a.tag == this.outClasses.toList(0))(0) // take an annotation of the current Tag and make sure it is in the training data
                       val currentAnnotationsPositiveWithoutOne = currentAnnotationsPositive.filter(a => a != annotCurrentClass)
                       r.shuffle(currentAnnotationsPositiveWithoutOne).take((currentAnnotationsPositiveWithoutOne.length*0.8).toInt).toSet ++ Set(annotCurrentClass)
                     }
      val trainNeg = r.shuffle(currentAnnotationsNegative).take((currentAnnotationsNegative.length*0.7).toInt).toSet
      val train = r.shuffle(trainPos ++ trainNeg)
      val test = (r.shuffle(currentAnnotationsPositive.toSet -- trainPos) ++ (currentAnnotationsNegative.toSet -- trainNeg)).toList

      // println("currentAnnotationsNegative:"+currentAnnotationsNegative.size)
      // //currentAnnotationsNegative.foreach(a => if(a.tokens.size < 5) println(a.tokens+"; tag: "+a.tag+"; inRel:"+a.inRel) else println(a.tokens.take(8)+"; tag: "+a.tag+"; inRel:"+a.inRel))
      // println("train positive samples:"+trainPos.size)
      // //trainPos.foreach(a => if(a.tokens.size < 5) println(a.tokens) else println(a.tokens.take(8)))
      // println("train negative samples:"+trainNeg.size)
      // //trainNeg.foreach(a => if(a.tokens.size < 5) println(a.tokens) else println(a.tokens.take(8)))
      // println()
      // println("test positive samples:"+(currentAnnotationsPositive.toSet -- trainPos).size)
      // println("test negative samples:"+(currentAnnotationsNegative.toSet -- trainNeg).size)
      //println((currentAnnotationsNegative.toSet -- trainNeg).toList.map(a => a.setInRel(false)).size)
      val newParams = this.params.cloneWith(None, true) match {
        case Some(value) => value
        case None => throw new Exception("ERROR: cloneWith current classifier node returned None in function evaluateMetrics")
      }
      newParams.annotations ++= train.map(_.toAnnotation)
      val trainNode = newParams.toNode(vectorIndex = index).asInstanceOf[ClassifierNode]

      val trainClasses = (c:Int) =>
        (for(i<-It.range(0, trainNode.points.size))
          yield(
            trainNode.rel(c).get(i) match {
                case Some(from) => if (trainNode.inRel(c)((i, from))) c else -1
                case _ if posClasses.exists( pc => !trainNode.rel.get(pc).isEmpty && !trainNode.rel(pc).get(i).isEmpty) => c // translate original positiv classes to current class
                case _ => -1
           })
        ).toSeq
         .zip(trainNode.points)
         .flatMap{case(c, p) => if(p == null) None else Some(c)}

      val trainPoints = trainNode.points.filter( _!= null)
      for(c <- this.outClasses) {
        // maybe hardcode 6 tweets to test
          // println("")
          // println("This Node: "+this.params.name)
          // println("posClasses:")
          // println(posClasses)
          // println("currentAnnotationsPositive:")
          // currentAnnotationsPositive.foreach(a => if(a.tokens.size < 5) println(a.tokens+"; tag: "+a.tag+"; inRel:"+a.inRel) else println(a.tokens.take(8)+"; tag: "+a.tag+"; inRel:"+a.inRel))
          // println("currentAnnotationsNegative:")
          // currentAnnotationsNegative.foreach(a => if(a.tokens.size < 5) println(a.tokens+"; tag: "+a.tag+"; inRel:"+a.inRel) else println(a.tokens.take(8)+"; tag: "+a.tag+"; inRel:"+a.inRel))
          // println("")
          // println("train positive samples:")
          // trainPos.foreach(a => if(a.tokens.size < 5) println(a.tag+":"+a.tokens) else println(a.tag+":"+a.tokens.take(8)))
          // println("test positive:")
          // (currentAnnotationsPositive.toSet -- trainPos).toList.foreach(a => if(a.tokens.size < 5) println(a.tag+":"+a.tokens) else println(a.tag+":"+a.tokens.take(8)))

          // println()
          // println("train negative samples:")
          // trainNeg.foreach(a => if(a.tokens.size < 5) println(a.tokens) else println(a.tokens.take(8)))
          // println()
        //
        // def findVectorForSentence(sentence:String) = {
        //   val sentence_vec = index match {
        //      case Some(ix) => ix(sentence.split("(?!^)\\b").toSeq.distinct) match {
        //                         case map => sentence.split("(?!^)\\b").toSeq.flatMap(token => map.get(token.toString)).reduceOption(_.sum(_)).getOrElse(null)}
        //      case None => throw new Exception("Provided vectorIndex is None!")}
        //
        //   trainPoints.zipWithIndex.filter{ case (vec, i) => vec.cosineSimilarity(sentence_vec) > 0.995}
        // }
        //
        // val pos_node_vec = findVectorForSentence("coma, diabetic, glucose")
        // val neg_node_vec = findVectorForSentence("hell, I, next")
        // val pos_node_child_vec = findVectorForSentence("@WheresMyStork The ONLY good thing about being diabetic is you get to skip the glucose test. But I probably would h… https://t.co/Pag0i3NXim")
        // val neg_node_child_vec = findVectorForSentence("diabetic, glucose, obese")
        // val pos_node_brother_vec = findVectorForSentence("the, it, really")
        // val neg_node_brother_vec = findVectorForSentence("insulin, diabetic, borderline")
        //
        // if (this.params.name == "Glucose") {
        //   println("This Node: "+this.params.name)
        //   println("size train points: "+trainPoints.size)
        //   println("Number of annotations in trainNode:"+trainNode.params.annotations.size)
        //   println("size class("+c+"): "+trainClasses(c).size)
        //
        //   if (pos_node_vec.size == 1) println("Pos sample has class: "+trainClasses(c)(pos_node_vec(0)._2)+" , expected: c")
        //   else if (pos_node_vec.size > 1) println("Pos sample has several matches: "+pos_node_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: c")
        //   else println("Pos sample has no class"+" , expected: c")
        //   if (neg_node_vec.size == 1) println("Neg sample has class: "+trainClasses(c)(neg_node_vec(0)._2)+" , expected: -1")
        //   else if (neg_node_vec.size > 1) println("Neg sample has several matches: "+neg_node_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: -1")
        //   else println("Neg sample has no class"+" , expected: -1")
        //   if (pos_node_child_vec.size == 1) println("Pos sample child has class: "+trainClasses(c)(pos_node_child_vec(0)._2)+" , expected: c")
        //   else if (pos_node_child_vec.size > 1) println("Pos sample child has several matches: "+pos_node_child_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: c")
        //   else println("Pos sample child has no class"+" , expected: c")
        //   if (neg_node_child_vec.size == 1) println("Neg sample child has class: "+trainClasses(c)(neg_node_child_vec(0)._2)+" , expected: no class")
        //   else if (neg_node_child_vec.size > 1) println("Neg sample child has several matches: "+neg_node_child_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: no class")
        //   else println("Neg sample child has no class"+" , expected: no class")
        //   if (pos_node_brother_vec.size == 1 ) println("Pos sample brother has class: "+trainClasses(c)(pos_node_brother_vec(0)._2)+" , expected: -1")
        //   else if (pos_node_brother_vec.size > 1) println("Pos sample brother has several matches: "+pos_node_brother_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: -1")
        //   else println("Pos sample brother has no class"+" , expected: -1")
        //   if (neg_node_brother_vec.size == 1 ) println("Neg sample brother has class: "+trainClasses(c)(neg_node_brother_vec(0)._2)+" , expected: no class")
        //   else if (neg_node_brother_vec.size > 1) println("Neg sample brother has several matches: "+neg_node_brother_vec.map{ case (vec,i) => trainClasses(c)(i)}.mkString(", ")+" , expected: no class")
        //   else println("Pos sample brother has no class"+" , expected: no class")
        //

        trainNode.models(c) = WrappedClassifier(
            forClass = c
            , points = trainPoints
            , pClasses = trainClasses(c)
            , spark = spark)

      }
      var output:ArrayBuffer[PerformanceReport] = ArrayBuffer.empty[PerformanceReport]
      // println("test pos:"+test.filter(a => a.inRel == true).size)
      // println("test neg:"+test.filter(a => a.inRel == false).size)
      // println("NODE: "+this.params.name)
      for(c <- this.outClasses) {
        val testDF = test.map { a =>
          val tokens = a.tokens
          //if(a.tokens.size < 5) println(a.inRel+": "+a.tokens) else println(a.inRel+": "+a.tokens.take(9))

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
        println("testDF:"+testDF.show(20))
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\n")
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nThis Node: "+this.params.name)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\n"+testDF.show(20))
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nPositive current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && a.inRel)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nNegative current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nPositive training samples (with brother/child):"+(train.filter(a => a.inRel == true)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nNegative training samples (with brother/child):"+(train.filter(a => a.inRel == false)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nPositive test samples (with brother/child):"+(test.filter(a => a.inRel == true)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nNegative test samples (with brother/child):"+(test.filter(a => a.inRel == false)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nPositive total (with brother/child):"+((train++test).filter(a => a.inRel == true)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\nNegative total (with brother/child):"+((train++test).filter(a => a.inRel == false)).size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\ncurrentAnnotationsPositive:"+currentAnnotationsPositive.size)
        // scala.tools.nsc.io.File("/home/adrian/PhD/Epiconcept/tmp/log.txt").appendAll("\ncurrentAnnotationsNegative:"+currentAnnotationsNegative.size)
        println("\nThis Node: "+this.params.name)
        println("Positive current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && a.inRel)).size)
        println("negative current node:"+allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel)).size)
        println("Positive training samples:"+(train.filter(a => a.inRel == true)).size)
        println("Negative training samples:"+(train.filter(a => a.inRel == false)).size)
        println("Positive test samples:"+(test.filter(a => a.inRel == true)).size)
        println("Negative test samples:"+(test.filter(a => a.inRel == false)).size)
        println("Positive total samples:"+((train++test).filter(a => a.inRel == true)).size)
        println("Positive total samples:"+((train++test).filter(a => a.inRel == false)).size)
        println("currentAnnotationsPositive:"+currentAnnotationsPositive.size)
        println("currentAnnotationsNegative:"+currentAnnotationsNegative.size)
        println("(train++test).filter(a => a.inRel == true)).size:"+(train++test).filter(a => a.inRel == true).size)
        println("(train++test).filter(a => a.inRel == true)).size.toDouble:"+(train++test).filter(a => a.inRel == false).size)
        println()


        val evaluator = new BinaryOptimalEvaluator().fit(testDF)
        val metrics = evaluator.metrics
        this.params.metrics =
          this.params.metrics ++ Map(s"prec${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.precision.getOrElse(0.0),
                                  s"recall${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.recall.getOrElse(0.0),
                                  s"f1${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.f1Score.getOrElse(0.0),
                                  s"AUC${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.areaUnderROC.getOrElse(0.0),
                                  s"accuracy${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.accuracy.getOrElse(0.0),
                                  s"pValue${if(this.outClasses.size > 1) s"_$c" else ""}" ->  metrics.pValue.getOrElse(0.0),
                                  s"threshold${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.threshold.getOrElse(0.0),
                                  s"positiveAnnotations${if(this.outClasses.size > 1) s"_$c" else ""}" -> ((train++test).filter(a => a.inRel == true)).size.toDouble,
                                  s"negativeAnnotations${if(this.outClasses.size > 1) s"_$c" else ""}" -> ((train++test).filter(a => a.inRel == false)).size.toDouble,
                                  s"tp${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.tp.getOrElse(0).toDouble,
                                  s"fp${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.fp.getOrElse(0).toDouble,
                                  s"tn${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.tn.getOrElse(0).toDouble,
                                  s"fn${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.fn.getOrElse(0).toDouble
                                )
        this.params.rocCurve ++ Map(s"rocCurve${if(this.outClasses.size > 1) s"_$c" else ""}" -> metrics.rocCurve)

        output += PerformanceReport(metrics.threshold
                                  , metrics.precision
                                  , metrics.recall
                                  , metrics.f1Score
                                  , metrics.areaUnderROC
                                  , metrics.rocCurve
                                  , metrics.accuracy
                                  , metrics.pValue
                                  , this.params.name
                                  , this.params.tagId
                                  , c
                                  , new Timestamp(System.currentTimeMillis())
                                  , allAnnotations.filter(a => (this.outClasses(a.tag) && a.inRel)).size
                                  , allAnnotations.filter(a => (this.outClasses(a.tag) && !a.inRel)).size
                                  , ((train++test).filter(a => a.inRel == true)).size
                                  , ((train++test).filter(a => a.inRel == false)).size
                                  , metrics.tp
                                  , metrics.fp
                                  , metrics.tn
                                  , metrics.fn
                                )
      }
      output
  }
  def mergeWith(that:Node, cGenerator:Iterator[Int], fit:Boolean):this.type = {
    this.params.hits = this.params.hits + that.params.hits
//    TODO: merge externalClassesFreq for this that
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
  , rocCurve:Array[(Double, Double)]=Array[(Double, Double)]()
  , accuracy:Option[Double]=None
  , pValue:Option[Double]=None
  , nodeName:String
  , tagId: Option[Int]
  , classId: Int
  , timestamp:Timestamp
  , positiveAnnotations: Int
  , negativeAnnotations: Int
  , positiveAnnotationsChildBrother: Int
  , negativeAnnotationsChildBrother: Int
  , tp:Option[Int]=None
  , fp:Option[Int]=None
  , tn:Option[Int]=None
  , fn:Option[Int]=None
)

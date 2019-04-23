package demy.mllib.topic

import demy.mllib.params._
import demy.mllib.tuning.{FoldsPredictorModel, BinaryOptimalEvaluator, RandomSplit}
import demy.mllib.classification.DiscreteVectorClassifier
import demy.mllib.linalg.implicits._
import demy.util.{log => l}
import demy.mllib.evaluation.RawPrediction2Score
import org.apache.spark.ml.{Model => SparkModel, Estimator}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.attribute.{AttributeGroup, Attribute} 
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.sql.{SparkSession, Dataset, DataFrame, Row}
import org.apache.spark.sql.functions.{expr, split, sum, lit, count, when, udf, col, concat, trim, coalesce, rand}
import org.apache.spark.sql.types._
import scala.util.Random
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}

trait VectorGeneticCategorizerBase extends Params with HasTokensCol with HasVectorsCol with HasOutputCol with HasPredictionCol with HasRawPredictionCol {
  final val populationSize = new Param[Int](this, "populationSize", "number of top phrases groups that will evolve on each iteration")
  final val nTopics = new Param[Int](this, "nTopics", "initial number of top phrases on each group")
  final val sentenceSize = new Param[Int](this, "sentenceSize", "number of trokens on each sentence")
  final val scoringVector = new Param[String](this, "scoringVector", "if a column name is provided, a score evaluating the ability to preduct this vecor base on categories will be output")
  setDefault(populationSize -> 100, nTopics -> 10, sentenceSize -> 10) 
  def setPopulationSize(value: Int): this.type = set(populationSize, value)
  def setNTopics(value: Int): this.type = set(nTopics, value)
  def setSentenceSize(value: Int): this.type = set(sentenceSize, value)
  def setScoringVector(value: String): this.type = set(scoringVector, value)
  def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
  def transformSchema(schema: StructType): StructType = {
    schema
      .add(new AttributeGroup(name=getOrDefault(rawPredictionCol)).toStructField)
      .add(new AttributeGroup(name=getOrDefault(predictionCol)).toStructField)
      .add(
        name = getOrDefault(outputCol)
        , dataType = 
          ArrayType(elementType =
            ArrayType(elementType = 
              StructType(Seq(
                StructField(name = "from", dataType = IntegerType, nullable = false)
                ,StructField(name = "to", dataType = IntegerType, nullable = false)
                ,StructField(name = "score", dataType = DoubleType, nullable = false)
                ))
            )
          ))
  }
  
  def  getSimilarity(readingVector:MLVector, topicVector:MLVector, readingHash:Int, topicHash:Int, cache:SimCache):Double = {
    cache.get(readingHash, topicHash) match {
      case Some(s) => s
      case None => 
        val s = readingVector.similarityScore(topicVector)
        cache.set(readingHash, topicHash, s)
        s
    }
  }
  def cHash(sent:Seq[String]):Int = {
    sent.sortWith(_ < _).foldLeft(0)((curr, next)=> curr + next.hashCode)
  }
  def getNextGeneration(currents:Seq[(Seq[(Seq[String], MLVector, Double)], Double)], cache:SimCache):Seq[Seq[(Seq[String], MLVector)]] = {
    val nMatches = currents.size - 1
    val probSlots = nMatches * (nMatches + 1) / 2
    val r = new Random()
    Range(0, currents.size).map{case i => 
      val rand = r.nextInt(probSlots)
      val pIndex = 
        (-0.5 + math.sqrt(1 + 8 * (probSlots - rand -1 ))/2.0).toInt //quad root of nMatches(nMatches+1)/2 = rannd
          match {
            case slot if slot < i => slot 
            case slot if slot >= i => slot + 1
          }  
      val mixedSentences = (currents(i)._1 ++ currents(pIndex)._1).sortWith(_._3 > _._3) 
      val chosens = scala.collection.mutable.HashSet[Int](0)
      val availables = scala.collection.mutable.HashSet[Int](Range(1, mixedSentences.size):_*)
      val childSentences = Range(0, currents(i)._1.size).map{ _ =>
        val farestAvailable = availables
          .map{aI =>
            val closestChosen = chosens.map{cI => (cI, getSimilarity(mixedSentences(aI)._2, mixedSentences(cI)._2, cHash(mixedSentences(aI)._1), cHash(mixedSentences(cI)._1), cache))}
              .reduce((p1, p2) => (p1, p2) match {case ((_, sim1), (_, sim2)) => if(sim1 > sim2) p1 else p2 })
            (aI, closestChosen._2)
          }
          .reduce((p1, p2) => (p1, p2) match {case ((_, sim1), (_, sim2)) => if(sim1 < sim2) p1 else p2 })
        val chosenIndex = farestAvailable._1
        chosens += chosenIndex
        availables -= chosenIndex
        mixedSentences(chosenIndex) match {case (tokens, vector, score) => (tokens, vector)}
      }.toSeq
      childSentences
    }.toSeq
  }
  def slideVectors(vectors:Seq[MLVector], windowSize:Int):Iterator[(MLVector, Int, Int)] = {
    var slidingV = None.asInstanceOf[Option[(MLVector, Int, Int)]]
    var start = 0
    var current = 0
    vectors.iterator.flatMap{ case(reading) => 
      slidingV = (slidingV, reading) match {
        case (None, null) => None
        case (None, _) => Some((reading, start, current + 1)) 
        case (Some((v, _, _)), null) => Some((v, start, current + 1)) 
        case (Some((v, _, _)), _) => Some((v.sum(reading), start, current + 1))
      }

      if (current > windowSize) {
        slidingV = (slidingV, vectors(start)) match {
          case (None, null) => None
          case (None, _) => throw new Exception("error @epi should not get on this case")
          case (Some((v, _, _)), null) => Some((v, start, current + 1)) 
          case (Some((v, _, _)), _) => Some((v.minus(vectors(start)), start, current + 1 ))
        }
        start = start + 1
      }

      current = current + 1
      if(current >= windowSize)
        slidingV
      else
        None
    }
  }
  
  def findBestSentence(vector:MLVector, vHash:Int, topSentences:Seq[(Seq[String], MLVector)], cache:SimCache):(Int, Double) = {
    val (bestJ, bestSim, sumSim) = topSentences
      .zipWithIndex
      .map{ case ((tokens, sentVector), j) => (j, getSimilarity(readingVector = vector, topicVector = sentVector, readingHash = vHash, topicHash = cHash(tokens), cache))}
      .map{ case (j, sim) => (j, sim, sim)}
      .reduce((p1, p2) => (p1, p2) match { 
        case ((closestJ1, closestSim1, sumSim1),(closestJ2, closestSim2, sumSim2)) =>
          (if(closestSim1 > closestSim2) closestJ1 else closestJ2 
            , if(closestSim1 > closestSim2) closestSim1 else closestSim2
            , sumSim1 + sumSim2
          )})
      (bestJ, bestSim - ((sumSim-bestSim)/(topSentences.size-1)))
  }

  
  def  evaluatePerformance(ds:Dataset[_], manualTags:String, vectors:String):Double = {
    val vectorType = ds.schema(vectors).dataType      
    val df = ds
      .where(col(manualTags).isNotNull)
      .select(col(manualTags), udf((v:Object)=>{
        if(v == null) 
          null.asInstanceOf[MLVector]
        else 
          v match {
            case ml:MLVector => ml
            case a:Iterable[_] => 
              val noNull = a.filter(_ != null)
              if(noNull.isEmpty)
                null.asInstanceOf[MLVector]
              else noNull.head match {
                case d:Double => Vectors.dense(a.map(_.asInstanceOf[Double]).toArray)
                case d:MLVector => noNull.map(_.asInstanceOf[MLVector]).reduce(_.sum(_))
	        case t => throw new Exception(s"Incompatible array of (${noNull.head.getClass.getName}) type to transform as vector for evaluating performance @epi")
              }
	    case _ => throw new Exception(s"Incompatible type (${v.getClass.getName}) to transform as vector for evaluating performance @epi")
           }
      }).apply(col(vectors)).as(vectors))
      .where(col(vectors).isNotNull)
    val rpCol = "rawPredictionPerformance"
    val pCol = "predictionPerformance"
    val nOpCol = "nonOptPredictionPerformance"
    val scCol = "scorePerformance"

    val predictedDF  = new FoldsPredictorModel()
      .setFeaturesCol(vectors)
      .setLabelCol(manualTags)
      .setRawPredictionCol(rpCol)
      .setPredictionCol(nOpCol)
      .setEstimator(new DiscreteVectorClassifier()
        .setLabelCol(manualTags)
        .setFeaturesCol(vectors)
        .setParallelism(4)
        .setClassifier(
          new LinearSVC()
            //.setMaxIter(50)
            .setRegParam(0.1)
            .setTol(0.001)
            //.setSeed(10L)
        ))
      .setFolder(new RandomSplit().setTrainRatio(0.8))
      .transform(df) match {case df =>
        new RawPrediction2Score()
          .setRawPredictionCol(rpCol)
          .setScoreCol(scCol)
          .transform(df)
      }

    val evaluator = new BinaryOptimalEvaluator()
      .setScoreCol(scCol)
      .setPredictionCol(pCol)
      .setLabelCol(manualTags)
      .setBins(500)    
      .setOptimize("f1Score") 
      .fit(predictedDF)

    val metrics = evaluator.metrics
    val res = evaluator.transform(predictedDF)
//    res.take(1000).foreach(r => {
//      println("--------00000-------------")
//      res.columns.foreach{c => println(s"$c: ${r.getAs[Any](c)}")}
//    }) 
    l.msg(metrics.report)
    metrics.f1Score.get
  }
}

class VectorGeneticCategorizer(override val uid: String) extends Estimator[VectorGeneticCategorizerModel] with VectorGeneticCategorizerBase {

  def this() = this(Identifiable.randomUID("VectorGeneticCategorizer"))
  override def fit(ds: Dataset[_]):VectorGeneticCategorizerModel = {
    //Top phrases fit algorithm:
    //  - Top Phrase Score
    //  - Top phtrases score = sum of phrases scores
    //Iteration Step:
    val spark = ds.sparkSession
    import spark.implicits._
    val sc = spark.sparkContext
    val texts = ds.select(getOrDefault(tokensCol), getOrDefault(vectorsCol)).as[(Seq[String], Seq[MLVector])]
    val popSize = getOrDefault(populationSize)
    val sentSize = getOrDefault(sentenceSize)
    val nTop = getOrDefault(nTopics)

    //Evaluating base performance
    val referenceScore = get(scoringVector) match {
      case Some(scoreCol) => Some(this.evaluatePerformance(ds = ds, manualTags = scoreCol, vectors = getOrDefault(vectorsCol)))
      case _ => None
    }

    var iterationScore = 0.0
    var improvement = Double.MaxValue
    var newPopulation =  sc.broadcast(buildInitialPopulation(texts = texts, popSize = popSize, sentSize= sentSize, nTop = nTop))
    var bestPopulation = Seq[(Seq[(Seq[String], MLVector, Double)], Double)]()
    while(improvement > 0) {
      l.msg("Evaluating individuals")
      var scoringResult = texts
        .mapPartitions{ iter =>
          val cache = new SimCache()
          val partitionScores = newPopulation.value.map(topSentences => topSentences.map{case(tokens, vector) => (0.0, tokens, vector, 0.0)}.toArray)
          iter.foreach{ case (textTokens, textVectors) => 
            this.slideVectors(vectors = textVectors, windowSize = sentSize).foreach { case (read, start, until) => 
              newPopulation.value.iterator.zipWithIndex.foreach{ case (topSentences, i) => 
                val (bestJ, readScore) = 
                  this.findBestSentence(
                    vector=read 
                    , vHash = cHash(textTokens.slice(start, until))
                    , topSentences = topSentences
                    , cache = cache
                  )
                
                partitionScores(i)(bestJ) = partitionScores(i)(bestJ) match {
                  case (topSentScore, bestChildTokens, bestChildVector, bestChildScore ) => 
                    (topSentScore + readScore / sentSize
                      ,if(readScore > bestChildScore) textTokens.slice(start, until) else bestChildTokens
                      ,if(readScore > bestChildScore) read else bestChildVector
                      ,if(readScore > bestChildScore) readScore else bestChildScore
                    )
                }
              }
            }
          }
          //Returning the partition calculation as a single iterator
          Some(partitionScores).iterator
        }
        .reduce{(score1, score2) =>
          score1.zip(score2).map{case (groupIn1, groupIn2) => groupIn1.zip(groupIn2).map{
            case ((topSentScore1, bestChildTokens1, bestChildVector1, bestChildScore1),(topSentScore2, bestChildTokens2, bestChildVector2, bestChildScore2)) => 
              (topSentScore1 + topSentScore2
                ,if(bestChildScore1 > bestChildScore2) bestChildTokens1 else bestChildTokens2
                ,if(bestChildScore1 > bestChildScore2) bestChildVector1 else bestChildVector2
                ,if(bestChildScore1 > bestChildScore2) bestChildScore1 else bestChildScore2
              )
          }}
        }

        val newPopScored = newPopulation.value
          .zip(scoringResult)
          .map{case (sentenceGroup, scores) => 
            ( sentenceGroup.zip(scores)
                .map{case ((tokens, vector), (score, bestChildTokens, bestChildVector, bestChildScore)) => 
                  //if(tokens.toSeq != bestChildTokens.toSeq) println(s"CHANGE!!!! : $tokens --> $bestChildTokens")
                  (bestChildTokens, bestChildVector, score)
                }
                .sortWith{_._3 > _._3} //this will make best sentences find closests partners before others */
              ,scores.map(_._1).reduce(_ + _)
            )
          }

        l.msg("Choosing the bests")
        val cache = new SimCache()
        bestPopulation = (bestPopulation ++ newPopScored)
          .sortWith(_._2 > _._2)
          .take(popSize)
                          //println(readScore)
                          //partitionScores.foreach{case s => println("Top Sentence: ");s.foreach{ case (score, tokens, vectors, sentScore) => println(s"$score --> $tokens")}}
                          //newopulation.value.foreach{case s => println("Top Sentence: ");s.foreach{ case (tokens, vectors) => println(tokens)}}
                          bestPopulation.take(1).foreach{case (s, score) => println(s"Top Sentence: $score");s.foreach{ case (tokens, vectors, scor) => println(s"$scor --> $tokens")}}
        val oldScore = iterationScore
        iterationScore = bestPopulation.map(_._2).reduce(_ + _)
        improvement = (iterationScore - oldScore)
        newPopulation.destroy
        l.msg("Getting next generation")
        newPopulation = sc.broadcast(getNextGeneration(bestPopulation, cache))
        l.msg($"Iteration End: score = ${iterationScore}")
        //l.msg($"Cache score = ${SimCache.hits.toDouble / (SimCache.hits + SimCache.noHits) }")

    }
    newPopulation.destroy
    val topSentences  = bestPopulation.head match {case (s, score) => s.map{ case (tokens, vector, score) => (tokens, vector)}}
    l.msg(s"top Sentences is : ${topSentences.map(_._1)}")
    copyValues(new VectorGeneticCategorizerModel(uid = uid, topSentences = topSentences, referenceScore = referenceScore).setParent(this))
  }
  /*case class Matrix(values:Array[Array[Double]]
  case class Tree(nodes:Seq[Node], trans:, freq:MLVector)
  case class Node(vector[MLVector], children:MLVector)*/

  //def buildInitialTrees(texts:Dataset[(Seq[String], Seq[MLVector])], childCount:Int, deep:Int):Seq[Tree] = {
  def buildInitialPopulation(texts:Dataset[(Seq[String], Seq[MLVector])], popSize:Int, sentSize:Int, nTop:Int) = {
    val spark = texts.sparkSession
    import spark.implicits._

    texts
      .flatMap{p => p match {case (tokens, vectors) => 
        val noNulls = tokens.zip(vectors).filter(_._2 != null)
        if(noNulls.size > sentSize) {
          Range(0, popSize).map{ i => 
            val sentenceStart = if(noNulls.size > sentSize) (new Random).nextInt( noNulls.size - sentSize ) else 0  
            (noNulls.slice(sentenceStart, sentenceStart + sentSize).map(_._1)
              , noNulls.slice(sentenceStart, sentenceStart + sentSize).map(_._2).reduce{(v1, v2) => v1.sum(v2)}
            )
          }
        }
        else Seq[(Seq[String], MLVector)]()
      }}
      .orderBy(rand)
      .take(popSize * nTop)
      .zipWithIndex
      .map{case ((tokens, vector), i) => (tokens, vector, i % popSize)}
      .groupBy{case (tokens, vector, i) => i}
      .values
      .map{s => s.map{case (tokens, vector, i) => (tokens, vector)}.toSeq}
      .toSeq
  }

  def refinePopulation(texts:Dataset[(Seq[String], Seq[MLVector], Option[Int])], freq:MLVector, tagsFreq:Seq[MLVector], atoms:Seq[(Seq[String], MLVector, MLVector)]) = {
    //each sentence can have a tag
    //each atoms can have a group of tags first vectors sum of words, second is tag scores
    //We are looking for atoms that strongly associate with its tags and that strongly separate untagged sentences
    //mapPartitions
    //for each atom take a set of candidates that are closest to him than any other
    //give a higher score if they better associate to a particular tag
    //increase size as possible
  }
}

class VectorGeneticCategorizerModel(
  override val uid: String
  , val topSentences:Seq[(Seq[String], MLVector)]
  , val referenceScore:Option[Double]
  ) extends SparkModel[VectorGeneticCategorizerModel] with VectorGeneticCategorizerBase 
{
  //def this() = this(Identifiable.randomUID("VectorGeneticCategorizerModel"))
  override def transform(ds: Dataset[_]): DataFrame = {
    val vIndex =  ds.schema.fieldIndex(getOrDefault(vectorsCol))
    val tIndex =  ds.schema.fieldIndex(getOrDefault(tokensCol))
    val sentSize =  getOrDefault(sentenceSize)
    import ds.sparkSession.implicits._ 
    val resDF = ds.sparkSession.createDataFrame(
      rowRDD = 
        ds.toDF.rdd.mapPartitions{it =>
          val cache = new SimCache()
          it.map{row =>
            val textVectors = row.getSeq[MLVector](vIndex)
            val tokensVectors = row.getSeq[String](tIndex)
            val outScores = Array.fill(topSentences.size){0.0}
            val outCategories = Array.fill(topSentences.size){0.0}
            val outSentences = Array.fill(topSentences.size){Seq[Row]()}
            var prevGroup = -1
            var prevScore = 0.0
            var prevStart = -1
            var prevTo = -1

            slideVectors(vectors = textVectors, windowSize = sentSize).foreach{ case (vRead, from, to) =>
              val (thisGroup, thisScore) = findBestSentence(vector = vRead, vHash = cHash(tokensVectors.slice(from, to)), topSentences = topSentences, cache = cache)
              //The group has changed, previous group needs to be registered
              var change = false
              if(prevGroup != -1 && prevGroup != thisGroup) {
                outScores(prevGroup) = if(prevScore > outScores(prevGroup)) prevScore else outScores(prevGroup)
                outSentences(prevGroup) = outSentences(prevGroup) :+ Row(prevStart, prevTo, prevScore)
                outCategories(prevGroup) = 1.0
                change = true
              } 
              //We are on the same group, we store information if we are on maximum
              else if(thisGroup != -1 && prevGroup == thisGroup && thisScore > prevScore) {
                change = true
              } 
              //if we are on last token we register current group
              if(thisGroup != -1 && to == textVectors.size) {
                outScores(thisGroup) = if(thisScore > outScores(thisGroup)) thisScore else outScores(thisGroup)
                outSentences(thisGroup) = outSentences(thisGroup) :+ Row(from, to, thisScore)
                outCategories(thisGroup) = 1.0
              }
              if(change) {
                prevGroup = thisGroup
                prevStart = from
                prevTo = to
              }
            }

            Row((row.toSeq ++ Seq(Vectors.dense(outScores), Vectors.dense(outCategories), outSentences)):_*)
          }
        }
      , schema = transformSchema(ds.schema)
    )
    //Evaluating obtained performance
    val referenceScore = get(scoringVector) match {
      case Some(scoreCol) => 
        resDF.cache
        println("atoms performance")
        val resPerf = Some(this.evaluatePerformance(ds = resDF, manualTags = scoreCol, vectors = getOrDefault(rawPredictionCol)))
        resDF.unpersist
        resPerf
      case _ => None
    }
    resDF
  }
}


class CachedSim(@volatile var sim:Option[Double], @volatile var newer:Option[Int], @volatile var older:Option[Int]) {
  override def toString = s"sim:$sim, newer:$newer, older:$older"
} 
class SimCache(val max:Int = 1000){ 
  lazy val cache = scala.collection.mutable.HashMap[Int, CachedSim]()
  val sem = new Object()
  @volatile var oldest:Option[Int] = None
  @volatile var newest:Option[Int] = None
  @volatile var hits = 0
  @volatile var noHits = 0
  def combine(hash1:Int, hash2:Int) = if(hash1 < hash2) (hash1, hash2).hashCode else (hash2, hash1).hashCode
  def get(hash1:Int,hash2:Int) = {
    if(hash1 == hash2) {
      hits = hits + 1
      Some(1.0)
    } else {
      combine(hash1, hash2) match { case (hash) =>
        touch(hash) match {
          case Some(s) => 
            //println(s"found $hash on ${cache.size}")
            hits = hits + 1
            Some(s)
          case None =>
            //println(s"not found $hash on ${cache.size}")
            noHits = noHits + 1
            None
        }
      } 
    }
  }
  def set(hash1:Int, hash2:Int, sim:Double) = {
    if(hash1 != hash2)
      combine(hash1, hash2) match { case (hash) =>
        //println(s"setting $hash")
        sem.synchronized {
          cache(hash).sim = Some(sim)
        }
      }
  } 
  def touch(hash:Int) = sem.synchronized {
    cache.get(hash) match {
      case(Some(cached)) =>
        if(Some(hash) == newest) {}//do nothing if current element is already the lastest
        else {//there an element before current (you are not newest)
          cached.newer match {
            case Some(n) => cache(n).older = cached.older
            case None => throw new Exception(s"Unexpected case @epi on $hash")
          }
          if(Some(hash) != oldest) {//if it is not the oldest
            cached.older match {
              case Some(o) => cache(o).newer = cached.newer
              case None => throw new Exception(s"Unexpected case @epi on $hash")
            }
          } else { //you are the oldest (and not the newest), so a new oldest needs to be defined
            oldest = cached.newer
          }
          //old newest newer needs to point to current
          newest match {
            case Some(n) => cache(n).newer = Some(hash) 
            case None => throw new Exception(s"Unexpected case @epi on $hash")
          }
          //and current older to previous newest
          cached.older = newest
        }
        newest = Some(hash)
        cached.sim
      case(None) =>
        //We are adding a new element we need to double link it with newest
        cache(hash) = new CachedSim(None, None, newest)
        newest match {
          case Some(n) if n != hash => 
            cache(n).newer = Some(hash)
          case Some(n) => throw new Exception(s"Unexpected case @epi on $hash")
          case _ =>
        }
        //We set newest 
        newest = Some(hash)
        //We set oldest if there is no oldest
        oldest match {
          case None => oldest = Some(hash)
          case _ => 
        }
  
        if(cache.size > max) { //drop oldest if cache is full
          val toRemove = oldest.get
          //println(s"removing $toRemove")
          val removed = cache.remove(toRemove).get
          oldest = removed.newer
          //println(s"new oldest $toRemove -> ${removed.newer}")
          removed.newer match {
            case Some(n) => cache(n).older = None
            case None => throw new Exception(s"Unexpected case @epi on $hash removing $toRemove")
          }
        }
        None
    }
  }

}


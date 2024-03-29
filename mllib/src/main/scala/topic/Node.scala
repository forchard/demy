package demy.mllib.topic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import demy.mllib.index.{CachedIndex}
import demy.storage.{FSNode, WriteMode}
import demy.mllib.evaluation.BinaryMetrics
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.sql.{SparkSession}
import org.apache.commons.io.IOUtils
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.io.{ObjectInputStream,ByteArrayInputStream}
import java.sql.Timestamp


/** Abstract class Node

 * @param params @tparam NodeParams parameters specific to the Node
 * @param points @tparam ArrayBuffer[MLVector] Word vectors
 * @param children @tparam ArrayBuffer[Node] Node children to this Node
 * @param links @tparam Map[Int, Set[Int]] Shows which class is transformed to which other class
 * @param classPath @tparam Map[Int, Set[Int]]
 * @param sequences Returns for each annotation tokens and from (Seq[String])
 * @param outClasses @tparam Set[Int] Out classes for this node
 * @param rel Hashmap (class, HashMap(index, from)) // index = index of vectors and class is the associated class
 * @param inRel same as rel, but boolean yes or no if annotation is pos or neg
 * @param inClasses
 * @param linkPairs
 * @param inMap
 */
trait Node{
  val params:NodeParams
  val points:ArrayBuffer[MLVector]
  val children:ArrayBuffer[Node]

  val links = this.params.strLinks.map(p => (p._1.toInt, p._2))
  val classPath = this.params.strClassPath.map(p => (p._1.toInt, p._2))
  val sequences = params.annotations.map(n => n.tokens) ++ params.annotations.flatMap(n => n.from) // clusteringNode: headword tokens
  lazy val outClasses = links.values.toSeq.flatMap(v => v).toSet
  lazy val rel = {
    val fromIndex = params.annotations.zipWithIndex.filter{case (n, i) => !n.from.isEmpty}.zipWithIndex.map{case ((_, i), j) => (i, params.annotations.size + j)}.toMap
    HashMap(
      params.annotations
       .zipWithIndex
       .map{case (n, i) => (n.tag, i, fromIndex.get(i).getOrElse(i))}
       .groupBy{case (tag, i, j) => tag}
       .mapValues{s => HashMap(s.map{case (tag, i, j) => (i, j)}:_*)}
       .toSeq :_*
   )
  }
  lazy val inRel = {
    val fromIndex = params.annotations.zipWithIndex.filter{case (n, i) => !n.from.isEmpty}.zipWithIndex.map{case ((_, i), j) => (i, params.annotations.size + j)}.toMap
    HashMap(
      params.annotations
       .zipWithIndex
       .map{case (n, i) => (n.tag, i, fromIndex.get(i).getOrElse(i), n.inRel)}
       .groupBy{case (tag, i, j, inRel) => tag}
       .mapValues{s => HashMap(s.map{case (tag, i, j, inRel) => ((i, j), inRel)}:_*)}
       .toSeq :_*
   )
  }
  lazy val inClasses = links.keySet
  lazy val linkPairs = links.toSeq.flatMap{case (from, toSet) => toSet.map(to => (from, to))}
  lazy val inMap = linkPairs.map{case (from, to) => (to, from)}.toMap

  def toTag(id:Int):TagSource
  def cloneUnfittedExtras:this.type
  def resetHitsExtras
  def updateParamsExtras
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String]
  def encodeExtras(encoder:EncodedNode)
  def mergeWith(that:Node, cGenerator:Iterator[Int], fit:Boolean):this.type

  // updates facts and scores for single node
  def transform(facts:HashMap[Int, HashMap[Int, Int]]
    , scores:HashMap[Int, Double]
    , vectors:Seq[MLVector]
    , tokens:Seq[String]
    , parent:Option[Node]
    , cGeneratror:Iterator[Int]
    , fit:Boolean)


  // "each sentences walks through tree" -> first classifierNodes then clusteringNodes  (runs in parallel, each thread has its own count/hits, merge in the end 'mergeWith')
  def walk(facts:HashMap[Int, HashMap[Int, Int]]
    , scores:HashMap[Int, Double]
    , vectors:Seq[MLVector]
    , tokens:Seq[String]
    , parent:Option[Node]
    , cGenerator:Iterator[Int]
    , fit:Boolean) {

    this.params.hits = this.params.hits + 1 // counts number of documents that passed through this node
    transform(facts, scores, vectors, tokens, parent, cGenerator, fit) // updates scores and facts
    val order = Seq.range(0, this.children.size) // needs to evaluate first classifiers and in the end clustering brother
      .sortWith((a, b) =>
        if(this.children(a).params.algo == this.children(b).params.algo)
          a < b
        else if(this.children(a).params.algo == ClassAlgorithm.clustering)
          false
        else
          true
      )

    for(o <- It.range(0, this.children.size)) {
      val i = order(o)
      val factClasses = facts.keySet.filter(k => facts(k).size > 0)
      val posIn = this.children(i).params.filterValue.toSet.filter(_ >=0)
      val negIn = this.children(i).params.filterValue.toSet.filter(_ < 0).map(-_) // negative filterValues in ClusteringBrother
      if(this.children(i).params.filterMode == FilterMode.noFilter
        || (this.children(i).params.filterMode == FilterMode.allIn && factClasses.intersect(negIn).isEmpty && posIn.subsetOf(factClasses))
        || (this.children(i).params.filterMode == FilterMode.anyIn && factClasses.intersect(negIn).isEmpty && !factClasses.intersect(posIn).isEmpty)
        || (this.children(i).params.filterMode == FilterMode.bestScore && {throw new Exception("best Score not supported anymore")})
      ){
        this.children(i).walk(facts, scores, vectors, tokens, Some(this), cGenerator, fit)
      }

    }
  }

  def clusteringGAP:Double = {
    this match {
      case n:ClusteringNode => n.leafsGAP
      case n if n.children.size > 0 => n.children.map(c => c.clusteringGAP).sum
      case _ => 0.0
    }
  }
  def fitClassifiers(spark:SparkSession, excludedNodes:Seq[Node] = Seq[Node]())  {
    this match {
      case n:AnalogyNode => n.fit(spark)
      case n:ClassifierNode => n.fit(spark, excludedNodes)
      case _ =>
    }
    this.children.zipWithIndex.foreach{case (n, i) => n.fitClassifiers(spark, excludedNodes ++ this.children.zipWithIndex.flatMap{case(n , j) => if(i == j) None else Some(n)})}
  }
  def evaluateClassifiers(spark:SparkSession, annotations:Seq[AnnotationSource], index:Option[VectorIndex],excludedNodes:Seq[Node] = Seq[Node]()) : ArrayBuffer[PerformanceReport] = { // excludeNodes added automatically
    var metrics:ArrayBuffer[PerformanceReport] = ArrayBuffer.empty[PerformanceReport]
    this match {
      case n:ClassifierNode => {metrics ++= n.evaluateMetrics(index, annotations, spark, excludedNodes)}
      case _ =>
    }
    metrics ++= this.children.zipWithIndex.flatMap{
      case (n, i) => n.evaluateClassifiers(spark, annotations, index, excludedNodes ++ this.children.zipWithIndex.flatMap{case(n , j) => if(i == j) None else Some(n)})
    }
    return metrics
  }

  def nodesIterator:Iterator[Node] = {
    It(this) ++ (for{i <- It.range(0, this.children.size)} yield this.children(i).nodesIterator).reduceOption(_ ++ _).getOrElse(It[Node]())
  }
  def userDefinedNodesIterator:Iterator[Node] = {
    It(this) ++ (for{i <- It.range(0, this.children.size) if this.params.algo != ClassAlgorithm.clustering } yield this.children(i).userDefinedNodesIterator).reduceOption(_ ++ _).getOrElse(It[Node]())
  }
  def leafsIteartor = nodesIterator.filter(n => n.children.size == 0)

  def encode(childArray:ArrayBuffer[EncodedNode]):Int= {
    this.updateParams(id = Some(childArray.size), updateChildren = false)
    val encoder =
      EncodedNode(
        points = this.points
        , params = this.params
      )
    encodeExtras(encoder)
    val index = childArray.size
    childArray += encoder
    encoder.children ++= this.children.map(_.encode(childArray))
    index
  }
  def serialize(o:Any) = {
    val serData=new ByteArrayOutputStream();
    val out=new ObjectOutputStream(serData);
    out.writeObject(o);
    out.close();
    serData.close();
    serData.toByteArray()
  }
  def prettyPrint(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String] = {
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> name: ${params.name}\n")
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> algo: ${params.algo}\n")
    prettyPrintExtras(level = level, buffer = buffer, stopLevel = stopLevel)
    if(stopLevel == -1 || level <= stopLevel)
      this.children.foreach(c => c.prettyPrint(level = level + 1, buffer = buffer, stopLevel = stopLevel))
    buffer
  }
  def betterThan(that:Node) = {
    val thisGap = this.clusteringGAP
    val thatGap = that.clusteringGAP
    val thisEmpty =  this.nodesIterator.filter(n => n.points.size < 2).size
    val thatEmpty =  that.nodesIterator.filter(n => n.points.size < 2).size

    //if(this.algo == ClassAlgorithm.supervised)  println(s"thisGap $thisGap, thatGap: $thatGap, thisEmpty $thisEmpty, thatEmpty $thatEmpty")
    (thisEmpty + thatEmpty > 0 && thisEmpty != thatEmpty) && thisEmpty < thatEmpty ||
    (thisEmpty + thatEmpty == 0 || thisEmpty == thatEmpty) && thisGap < thatGap
  }
  def resetHits:this.type = {
    this.params.hits = 0.0
    this.resetHitsExtras
    It.range(0, this.children.size).foreach(i => this.children(i).resetHits)
    this
  }
  def cloneUnfitted:this.type = {
    val ret = this.cloneUnfittedExtras
    ret.params.hits = 0.0
    val oldChildren = ret.children.clone
    ret.children.clear
    ret.children ++= oldChildren.map(c => c.cloneUnfitted)
    ret
  }

  def save(dest:FSNode, tmp:Option[FSNode]=None) = {
    this.updateParams(Some(0))
    val encoded = ArrayBuffer[EncodedNode]()
    this.encode(encoded)
    val bytes = this.serialize(encoded)
    tmp match {
      case Some(t) =>
        t.setContent(new ByteArrayInputStream(bytes), WriteMode.overwrite)
        t.move(dest, WriteMode.overwrite)
      case None =>
        dest.setContent(new ByteArrayInputStream(bytes), WriteMode.overwrite)
    }
  }

  def saveAsJson(dest:FSNode, tmp:Option[FSNode]=None) = {
    this.updateParams(Some(0))
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    mapper.enable(SerializationFeature.INDENT_OUTPUT)
    val encoded = ArrayBuffer[EncodedNode]()
    this.encode(encoded)
    tmp match {
      case Some(t) =>
        //println(s"writing json to ${dest.path} which is ${mapper.writeValueAsString(encoded)}")
        t.setContent(new ByteArrayInputStream(mapper.writeValueAsBytes(encoded.map(e => e.params))), WriteMode.overwrite)
        t.move(dest, WriteMode.overwrite)
      case None =>
        dest.setContent(new ByteArrayInputStream(mapper.writeValueAsBytes(encoded.map(e => e.params))), WriteMode.overwrite)
    }
  }
  def getClassGenerator(max:Option[Int]) = It.range(this.nodesIterator.flatMap(n => n.links.keys.iterator ++ n.links.values.iterator.flatMap(s => s)).max + 1, max.getOrElse(Int.MaxValue))
  def updateParams(id:Option[Int] = None, updateChildren:Boolean=true):Option[Int] = {

    val newAnnotations = (
      this.rel.flatMap{ case (outClass, rels) =>
        rels.map{case (iOut, iFrom) =>
          (iOut, iFrom, outClass)
        }
      }
       .toSeq
       .sortWith(_._1 < _._1)
       .map{case (iOut, iFrom, outClass) =>
         //print(s"--->($iOut, $iFrom, $outClass)")
         val ret = Annotation(
           tokens = this.sequences(iOut)
           , tag = outClass
           , from = if(iOut == iFrom) None else Some(this.sequences(iFrom))
           , inRel = this.inRel.get(outClass) match {case Some(map) => map(iOut -> iFrom) case None => true}
           , score = this match { case c:ClusteringNode =>c.pScores(iOut) case _ => 0.0}
         )
         ret
       }
    )
    this.params.annotations.clear
    this.params.annotations ++= newAnnotations

    updateParamsExtras
    var currentId = id
    if(updateChildren) {
      val childrenIds =
        It.range(0, this.children.size)
          .map{i =>
             val thisChildId = currentId.map(idd => idd + 1)
             currentId = this.children(i).updateParams(thisChildId)
             thisChildId
          }
      if(!currentId.isEmpty) {
        this.params.children.clear
        this.params.children ++= childrenIds.flatMap(d => d)
      }
    }
    currentId
  }

  def allSequences(ret:HashSet[Seq[String]]=HashSet[Seq[String]]()):HashSet[Seq[String]] = {
    ret ++= this.sequences
    this.children.foreach(c => c.allSequences(ret))
    ret
  }

  def getTopwordSum(filterValue:Int) : Option[MLVector] = {
    this.nodesIterator.filter{
      case node:ClusteringNode => node.links.values.flatMap(n => n).toSet.contains(filterValue)
      case _ => false
    }.toSeq.headOption
    .map(n => n.rel(filterValue).keys.map(i => n.points(i)).toSeq.reduce(_.sum(_)))
  }

}

object Node {
 def load(from:FSNode, format:String = "binary") = {
   val nodes = EncodedNode.deserialize[ArrayBuffer[EncodedNode]](IOUtils.toByteArray(from.getContent))
   nodes(0).decode(nodes)
 }

 def loadFromJson(from:FSNode, vectorIndex:Option[VectorIndex]) = {
    val params = NodeParams.loadFromJson(from)
    val cachedIndex = vectorIndex.map(index => CachedIndex(index = index).setCache(params(0).allTokens(others = params).toSeq))
    params(0).toNode(others = params, vectorIndex = cachedIndex)
 }

 def defaultNode =
   NodeParams(
     name = "Explorer"
     , tagId = None
     , color = None
     , annotations = ArrayBuffer[Annotation]()
     , algo = ClassAlgorithm.clustering
     , strLinks = Map("0" -> Set(1, 2))
     , filterMode = FilterMode.bestScore
     , filterValue = ArrayBuffer(0)
     , maxTopWords = Some(5)
     , classCenters= Some(Map("1"->0, "2" -> 1))
     , childSplitSize = Some(50)
     , hits = 0.0
   ).toNode()
}

case class EncodedNode(
  points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , params:NodeParams
  , children: ArrayBuffer[Int] = ArrayBuffer[Int]()
  , serialized:ArrayBuffer[(String, Array[Byte])] = ArrayBuffer[(String, Array[Byte])]()
) {
  def deserialize[T](name:String) = {
    val bytes = this.serialized.find{case (n, bytes) => n == name }.map{case (n, bytes) => bytes} match {
      case Some(bytes) => bytes
      case _ => throw new Exception (s"Cannot find property to deserialize '$name'")
    }
    var obj:Any = null
    if (bytes!=null) {
      val in=new ObjectInputStream(new ByteArrayInputStream(bytes)) {
        override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
          try { Class.forName(desc.getName, false, getClass.getClassLoader) }
          catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
         }
       }
       obj = in.readObject()
       in.close()
    }
    obj.asInstanceOf[T]
  }

  def decode(others:ArrayBuffer[EncodedNode]):Node = {
    val n =
      if(this.params.algo == ClassAlgorithm.clustering)
        ClusteringNode(this)
      else if(this.params.algo == ClassAlgorithm.supervised)
        ClassifierNode(this)
      else if(this.params.algo == ClassAlgorithm.analogy)
        AnalogyNode(this)
      else throw new Exception(s"Unknown algorithm ${this.params.algo}")
    n.children ++= this.children.map(i => others(i).decode(others))
    n
  }

  def stripBinary = {
    EncodedNode  (
      points = points
      , params = params
      , children = children
      , serialized = ArrayBuffer[(String, Array[Byte])]()
    )
  }

  def prettyPrint(others:ArrayBuffer[EncodedNode], stopLevel:Int = -1) = {
    val n = this.decode(others)
    n.prettyPrint(stopLevel=stopLevel)
  }
}
object EncodedNode {
  def deserialize[T](bytes:Array[Byte]) = {
    var obj:Any = null
    if (bytes!=null) {
       val in=new ObjectInputStream(new ByteArrayInputStream(bytes)) {
         override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
           try { Class.forName(desc.getName, false, getClass.getClassLoader) }
           catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
          }
        }
       obj = in.readObject()
       in.close()
    }
    obj.asInstanceOf[T]
  }



}

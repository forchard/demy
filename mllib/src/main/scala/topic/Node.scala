package demy.mllib.topic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import demy.mllib.index.VectorIndex
import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import demy.storage.{FSNode, WriteMode}
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.sql.{SparkSession}
import org.apache.commons.io.IOUtils
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.io.{ObjectInputStream,ByteArrayInputStream}

case class Annotation(token:String, cat:Int, from:Option[String], inRel:Boolean)
case class NodeParams(
  name:String
  , annotations:ArrayBuffer[Annotation]
  , algo:ClassAlgorithm
  , strLinks:Map[String, Set[Int]] = Map[String, Set[Int]]()
  , names:Map[String, Int] = Map[String, Int]()
  , var filterMode:FilterMode = FilterMode.noFilter
  , filterValue:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , maxTopWords:Option[Int]=None
  , classCenters:Option[Map[String, Int]]=None
  , vectorSize:Option[Int] = None
  , pScores: Option[Array[Double]]
  , childSplitSize: Option[Int] = None
  , children: ArrayBuffer[Int] = ArrayBuffer[Int]()
  , var hits:Double = 0
) {
  //def getOutMap(init:Double) =  HashMap(outClasses.toSeq.map(v => (v, init)):_*) 
  def toNode(others:ArrayBuffer[NodeParams], vectorIndex:VectorIndex):Node = {
   
   val n =
      if(this.algo == ClassAlgorithm.clustering) 
        ClusteringNode(this, vectorIndex)
      else if(this.algo == ClassAlgorithm.supervised)
        ClassifierNode(this, vectorIndex)
      else if(this.algo == ClassAlgorithm.analogy)
        AnalogyNode(this, vectorIndex)
      else throw new Exception(s"Unknown algorithm ${this.algo}")
    n.children ++= this.children.map(i => others(i).toNode(others, vectorIndex))
    n
  }
  
}

case class ClassAlgorithm(value:String)
object ClassAlgorithm {
  val analogy = ClassAlgorithm("analogy")
  val supervised= ClassAlgorithm("supervised")
  val clustering = ClassAlgorithm("clustering")
}
case class FilterMode(value:String)
object FilterMode {
  val noFilter = FilterMode("noFilter")
  val allIn = FilterMode("allIn")
  val anyIn = FilterMode("anyIn")
}

trait Node{
  val params:NodeParams
  val points:ArrayBuffer[MLVector]
  val children:ArrayBuffer[Node]

  def walk(facts:HashMap[Int, HashMap[Int, Int]], scores:Option[HashMap[Int, HashMap[Int, Double]]]=None, vectors:Seq[MLVector], tokens:Seq[String], cGenerator:Iterator[Int]) { 
    this.params.hits = this.params.hits + 1
    transform(facts, scores, vectors, tokens, cGeneratror)
    
    for(i <- It.range(0, this.children.size)) {
      if(this.children(i).params.filterMode == FilterMode.noFilter
        || this.children(i).params.filterMode == FilterMode.allIn 
           &&  this.children(i).inClasses.iterator
                .filter(inChild => !facts.contains(inChild))
                .size == 0 
        || this.children(i).params.filterMode == FilterMode.anyIn 
           &&  this.children(i).inClasses.iterator
                .filter(inChild => facts.contains(inChild))
                .size > 0 
      )
      this.children(i).walk(facts, scores, vectors, tokens, cGeneratror)
    }
  }
  val links = this.params.strLinks.map(p => (p._1.toInt, p._2))
  val tokens = params.annotations.map(n => n.token) ++ params.annotations.flatMap(n => n.from) 
  val outClasses = links.values.toSeq.flatMap(v => v).toSet
  val rel = {
    val fromIndex = params.annotations.zipWithIndex.filter{case (n, i) => !n.from.isEmpty}.zipWithIndex.map{case ((_, i), j) => (i, params.annotations.size -1 + j)}.toMap
    HashMap(
      params.annotations
       .zipWithIndex
       .map{case (n, i) => (n.cat, i, fromIndex.get(i).getOrElse(i))}
       .groupBy{case (cat, i, j) => cat}
       .mapValues{s => HashMap(s.map{case (cat, i, j) => (i, j)}:_*)}
       .toSeq :_*
   )
  }
  val inRel = {
    val fromIndex = params.annotations.zipWithIndex.filter{case (n, i) => !n.from.isEmpty}.zipWithIndex.map{case ((_, i), j) => (i, params.annotations.size -1 + j)}.toMap
    HashMap(
      params.annotations
       .zipWithIndex
       .map{case (n, i) => (n.cat, i, fromIndex.get(i).getOrElse(i), n.inRel)}
       .groupBy{case (cat, i, j, inRel) => cat}
       .mapValues{s => HashMap(s.map{case (cat, i, j, inRel) => ((i, j), inRel)}:_*)}
       .toSeq :_*
   )
  }
  val inClasses = links.keySet
  val linkPairs = links.toSeq.flatMap{case (from, toSet) => toSet.map(to => (from, to))}

  def transform(facts:HashMap[Int, HashMap[Int, Int]], scores:Option[HashMap[Int, HashMap[Int, Double]]]=None, vectors:Seq[MLVector], tokens:Seq[String], cGeneratror:Iterator[Int])  
  def clusteringGAP:Double = {
    this match {
      case n:ClusteringNode => n.leafsGAP
      case n if n.children.size > 0 => n.children.map(c => c.clusteringGAP).sum
      case _ => 0.0
    }
  }
  def fitClassifiers(spark:SparkSession)  {
    this match {
      case n:AnalogyNode => n.fit(spark)
      case n:ClassifierNode => n.fit(spark)
      case _ => 0
    }
    this.children.foreach(n => n.fitClassifiers(spark))
  }
  def nodesIterator:Iterator[Node] = {
    It(this) ++ (for(i <- It.range(0, this.children.size)) yield this.children(i).nodesIterator).reduceOption(_ ++ _).getOrElse(It[Node]())
  }
  def leafsIteartor = nodesIterator.filter(n => n.children.size == 0)

  def encode(childArray:ArrayBuffer[EncodedNode]):Int= {
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
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String]
  def encodeExtras(encoder:EncodedNode)
  def mergeWith(that:Node):this.type
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

  def resetHitsExtras

  def save(dest:FSNode, tmp:Option[FSNode]=None) = {
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
}

object Node {
 def load(from:FSNode, format:String = "binary") = {
   val nodes = EncodedNode.deserialize[ArrayBuffer[EncodedNode]](IOUtils.toByteArray(from.getContent))
   nodes(0).decode(nodes)
 }

 def loadFromJson(from:FSNode, vectorIndex:VectorIndex) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val text = from.getContentAsString
    val params = mapper.readValue[ArrayBuffer[NodeParams]](text)
    params(0).toNode(others = params, vectorIndex = vectorIndex)
 }
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
       val in=new ObjectInputStream(new ByteArrayInputStream(bytes))
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
       val in=new ObjectInputStream(new ByteArrayInputStream(bytes))
       obj = in.readObject()
       in.close()
    }
    obj.asInstanceOf[T]
  }

}  

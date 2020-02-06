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
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.sql.{SparkSession}
import org.apache.commons.io.IOUtils
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.io.{ObjectInputStream,ByteArrayInputStream}


case class NodeParams(
  name:String
  , tagId:Option[Int] = None
  , color:Option[String] = None
  , annotations:ArrayBuffer[Annotation]
  , algo:ClassAlgorithm
  , strLinks:Map[String, Set[Int]] = Map[String, Set[Int]]()
  , strClassPath:Map[String, Set[Int]] = Map[String, Set[Int]]()
  , names:Map[String, Int] = Map[String, Int]()
  , var filterMode:FilterMode = FilterMode.noFilter
  , filterValue:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , maxTopWords:Option[Int]=None
  , windowSize:Option[Int]=None
  , classCenters:Option[Map[String, Int]]=None
  , var cError: Option[Array[Double]]=None
  , childSplitSize: Option[Int] = None
  , children: ArrayBuffer[Int] = ArrayBuffer[Int]()
  , var hits:Double = 0
) {
  def toNode(others:ArrayBuffer[NodeParams]= ArrayBuffer[NodeParams](), vectorIndex:Option[VectorIndex]= None):Node = {
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
  def cloneWith(classMapping:Option[Map[Int, Int]], unFit:Boolean = true) = {
    if(!classMapping.isEmpty && !(this.strLinks.keySet.map(_.toInt) ++ this.strLinks.values.flatMap(v => v).toSet ++ this.filterValue.filter(_ >=0).toSet).subsetOf(classMapping.get.keySet))
        None
    else {
      Some(NodeParams(
        name = this.name
        , color = this.color
        , tagId = if(unFit) None else this.tagId
        , annotations = if(unFit) ArrayBuffer[Annotation]() else this.annotations.clone
        , algo = this.algo
        , strLinks = classMapping match {
            case Some(classMap) => this.strLinks.map{case (inClass, outSet) => (/*classMap(*/inClass/*.toInt).toString*/, outSet.map(o => classMap(o))) }
            case None => strLinks
        }
        , strClassPath =  classMapping match {
            case Some(classMap) => this.strClassPath.map{case (inClass, parentSet) => (classMap(inClass.toInt).toString, parentSet ++ this.filterValue.filter(_>=0).map(c => classMap(c))) }
            case None => strClassPath
        }
        , names = this.names
        , filterMode = this.filterMode
        , filterValue = classMapping match {
            case Some(classMap) => this.filterValue.map(c => classMap.get(c).getOrElse(c)) 
            case None => filterValue.clone
        }
        , maxTopWords = this.maxTopWords
        , windowSize = this.windowSize
        , classCenters = classMapping match {
            case Some(classMap) => this.classCenters.map(cCenters => cCenters.map{ case(outClass, center) => (classMap(outClass.toInt).toString, center)})
            case None => classCenters
        }
        , childSplitSize = this.childSplitSize
        , children = if(unFit) ArrayBuffer[Int]() else this.children.clone
        , hits = if(unFit) 0.0 else  this.hits
      ))
    }
  }
  def allTokens(ret:HashSet[String]=HashSet[String](), others:Seq[NodeParams]):HashSet[String] = {
    ret ++= this.annotations.flatMap(a => a.tokens ++ a.from.getOrElse(Seq[String]()))
    this.children.foreach(i => others(i).allTokens(ret, others))
    ret
  }
  def allSequences(ret:HashSet[Seq[String]]=HashSet[Seq[String]](), others:Seq[NodeParams]):HashSet[Seq[String]] = {
    ret ++= this.annotations.map(a => a.tokens)
    ret ++= this.annotations.flatMap(a => a.from)
    this.children.foreach(i => others(i).allSequences(ret, others))
    ret
  }

}

object NodeParams {
 def loadFromJson(from:FSNode) = {
    val mapper = new ObjectMapper() with ScalaObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val text = from.getContentAsString
    mapper.readValue[ArrayBuffer[NodeParams]](text)
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
  val bestScore = FilterMode("bestScore")
}


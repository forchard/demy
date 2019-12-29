package demy.mllib.topic
import org.apache.spark.sql.{Dataset}
import org.apache.spark.sql.functions.{col}
import java.sql.Timestamp 
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}

case class TagOperation(op:String)
object TagOperation {
  val create = TagOperation("create")
  val delete = TagOperation("delete")
  val update = TagOperation("update")
}

trait TagSource {
  val id:Int
  val operation:TagOperation
  var timestamp:Timestamp
  val name:String
  val filterMode:FilterMode
  val filterValue:Set[Int]
  val vectorSize:Int
  def outClasses(iClass:Int):Option[Set[Int]]
  def toSomeSource = SomeTagSource(this)
  def toNodeParams(children:Set[Int], classPath:Map[Int, Set[Int]]):NodeParams
  def resetTimestamp:this.type = {this.timestamp = new Timestamp(System.currentTimeMillis());this}
  def mergeWith(that:TagSource) = { 
    val (newer, older) = if(this.timestamp.after(that.timestamp)) (this, that) else (that, this)
    (older.operation, newer.operation) match {
      case (TagOperation.delete, TagOperation.update) => older
      case _ => newer
    }
  }
}

case class SomeTagSource(classifier:ClassifierTagSource, clustering:ClusterTagSource, analogy:AnalogyTagSource ) {
  val source:TagSource = if(clustering == null && analogy == null) classifier else if(clustering == null && classifier == null) analogy else clustering
  val resetTimestamp = {source.resetTimestamp;this}
}
object SomeTagSource {
  def apply(source:TagSource):SomeTagSource = source match {
    case a:ClassifierTagSource => SomeTagSource(classifier = a, clustering = null, analogy = null)
    case a:ClusterTagSource => SomeTagSource(classifier = null, clustering = a, analogy = null)
    case a:AnalogyTagSource => SomeTagSource(classifier = null, clustering = null, analogy = a)
    case _ => throw new Exception(s"Unexpected class for ${source}")
  }
}

object TagSource {
  def getTags(ds:Dataset[SomeTagSource]) = {
    import ds.sparkSession.implicits._
    ds.map(a => (a.source.id, a.source.timestamp, a))
      .repartition(col("_1"))
      .sortWithinPartitions(col("_2"))
      .mapPartitions{iter => 
        val ret = HashMap[Int, SomeTagSource]()
        iter.foreach{case (id, ts, a1) => 
          ret(id) = ret.get(id) match {
            case Some(a2) => SomeTagSource(a1.source.mergeWith(a2.source))
            case None => a1
          }
        }
        ret.values.iterator
      }
      .filter(sa => sa.source.operation != TagOperation.delete)
      .collect
      .map(sa => sa.source)
  }

  def getNodeParams(
    tags:Seq[TagSource]
    , nodes:ArrayBuffer[TagSource]=ArrayBuffer[TagSource]()
    , leafs:Seq[Int]=Seq[Int]()
    , children:ArrayBuffer[Set[Int]]= ArrayBuffer[Set[Int]]()
    , classPath:ArrayBuffer[Map[Int, Set[Int]]]= ArrayBuffer[Map[Int, Set[Int]]]()
  ):Seq[NodeParams] = {
    /*println(s"---------------------------")
    println(s"tags: $tags")
    println(s"nodes: $nodes")
    println(s"leafs: $leafs")
    println(s"children: $children")
    println(s"classPath: $classPath")*/
    val (newLeafs, rest) = 
      tags.map{tag => 
        if(tag.filterValue.isEmpty || tag.filterValue == Set(0)) {
          nodes += tag
          classPath += tag.outClasses(0).get.map(oClass => oClass -> Set(0)).toMap
          children += Set[Int]()
          ((Some(nodes.size-1), None))
        }
        else { 
          leafs.flatMap{iNode => 
            tag.filterValue
              .flatMap{fClass => 
                classPath(iNode)
                  .flatMap{case (oClass, parents) => 
                    val path = (parents + oClass)
                    if(fClass == oClass)
                      Some(path.flatMap(pClass => tag.outClasses(pClass)).head.map(cOut => (cOut -> path)))
                    else
                      None
                  }
                  .headOption.asInstanceOf[Option[Set[(Int, Set[Int])]]]
              }.asInstanceOf[Set[Set[(Int, Set[Int])]]] match {
                case s if s.filter(paths => paths.size > 0).size == tag.filterValue.size =>
                  nodes += tag
                  children += Set[Int]()
                  children(iNode) = children(iNode) + (nodes.size - 1)
                  classPath += s.flatMap(e => e).toMap
                  Some(nodes.size-1)
                case _ =>
                  None
              }
          }.headOption match {
            case Some(iTag) => ((Some(iTag), None))
            case None => (None, Some(tag)) 
          }
        }
      }
      .unzip match {case (s1, s2) => (s1.flatMap(s=>s), s2.flatMap(s=>s))}
    if(rest.size == 0) {
      nodes.zip(children.zip(classPath)).map{case(n, (ch, cPath)) => n.toNodeParams(ch, cPath)}
    } else if(rest.size ==  tags.size) {
      throw new Exception("Cannot obtain node tree representation, cannot match nodes to add as leafs and there are still nodes to be included")
    } else 
      getNodeParams(tags = rest, nodes = nodes, leafs = newLeafs, children = children, classPath = classPath)
  }
}

case class ClassifierTagSource(
  id:Int
  , operation:TagOperation
  , var timestamp:Timestamp = new Timestamp(System.currentTimeMillis())
  , name:String
  , inTag:Int
  , outTags: Set[Int]
  , oFilterMode:Option[FilterMode]
  , oFilterValue:Option[Set[Int]]
  , vectorSize:Int
) extends TagSource {
  val filterMode = this.oFilterMode.getOrElse(FilterMode.anyIn)
  val filterValue = this.oFilterValue.getOrElse(Set(inTag))
  def outClasses(iClass:Int) = if(iClass == inTag) Some(outTags) else None
  def toNodeParams(children:Set[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name
     , algo = ClassAlgorithm.supervised
     , strLinks = Map(inTag.toString -> outTags) 
     , filterMode = this.filterMode  
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , vectorSize = Some(this.vectorSize)
     , maxTopWords = None
     , classCenters= None
     , childSplitSize = None
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children.toSeq:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2)) 
   )
}
case class AnalogyTagSource(
  id:Int
  , operation:TagOperation
  , var timestamp:Timestamp = new Timestamp(System.currentTimeMillis())
  , name:String
  , referenceTag:Int
  , baseTag:Int
  , analogyClass:Int
  , oFilterMode:Option[FilterMode] = None
  , oFilterValue:Option[Set[Int]] = None
  , vectorSize:Int
) extends TagSource {
  val filterValue = this.oFilterValue.getOrElse(Set(baseTag,referenceTag))
  val filterMode = this.oFilterMode.getOrElse(FilterMode.allIn)
  def outClasses(iClass:Int) = if(iClass == baseTag) Some(Set(analogyClass)) else None 
  def toNodeParams(children:Set[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name
     , algo = ClassAlgorithm.analogy
     , strLinks = Map(baseTag.toString -> Set(analogyClass)) 
     , filterMode = this.filterMode 
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , vectorSize = Some(this.vectorSize)
     , maxTopWords = None
     , classCenters= None
     , childSplitSize = None
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children.toSeq:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2)) 
   )
}
case class ClusterTagSource (
  id:Int
  , operation:TagOperation
  , var timestamp:Timestamp = new Timestamp(System.currentTimeMillis())
  , name:String
  , strLinks:Map[String, Set[Int]] = Map[String, Set[Int]]()
  , maxTopWords:Int
  , vectorSize:Int
  , childSplitSize:Int
  , oFilterMode:Option[FilterMode] = None
  , oFilterValue:Option[Set[Int]] = None
) extends TagSource {
  val filterValue = this.oFilterValue.getOrElse(this.strLinks.keys.map(_.toInt).toSet)
  val filterMode = this.oFilterMode.getOrElse(FilterMode.allIn)
  def outClasses(iClass:Int) = strLinks.get(iClass.toString)
  def toNodeParams(children:Set[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name
     , algo = ClassAlgorithm.clustering
     , strLinks = strLinks
     , filterMode = this.filterMode
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , vectorSize = Some(this.vectorSize)
     , maxTopWords = Some(this.maxTopWords)
     , classCenters =  
         Some(Map(
           strLinks
             .flatMap{case (in, outs) => outs.zipWithIndex}
             .groupBy{case(out,center) => out.toString}
             .mapValues(p => p.head._2)
             .toSeq :_*
           ))
     , childSplitSize = Some(this.childSplitSize)
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children.toSeq:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2)) 
   )
}


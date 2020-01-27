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
  val addFilter = TagOperation("addFilter")
  val removeFilter = TagOperation("removeFilter")
  val fullOperations = Set(create, update)
}


/** Abstract Class for Tag object */
trait TagSource {
  /** Creates a Tag object
   * @param id @tparam Int The id of the tag
   * @param operation @tparam [[TagOperation]] The tag operation; for instance: "create", "delete" or "update"
   * @param timestamp @tparam Option[Timestamp] The time stamp of the tag
   * @param name @tparam Option[String] The name of the tag
   * @param color @tparam Option[String] The color of the tag
  **/
  val id:Int
  val operation:TagOperation
  var timestamp:Option[Timestamp]
  val name:Option[String]
  val color:Option[String]
  /** Filter mode to decide which sentence/token goes in which node, for instance: "bestScore" */
  def filterMode:FilterMode
  /** Describing the nodes whose sentences/tokens are to be filtered */
  def filterValue:Set[Int]
  /** TODO
    *
    * @param iClass @tparam Int TODO
    */
  def outClassesFor(iClass:Int):Option[Set[Int]]
  /** TODO
    */
  def outClasses():Set[Int]
  /** Transforms the instance to SomeTagSource, which can be used as a dataset
    */
  def toSomeSource = SomeTagSource(this)
  /** Transforms the instance to NodeParam, which can be used to create the stree structure
    */
  def toNodeParams(children:Seq[Int], classPath:Map[Int, Set[Int]]):NodeParams
  /** Resets th timestamp to the current time
    */
  def resetTimestamp:this.type = {this.timestamp = Some(new Timestamp(System.currentTimeMillis()));this}
  /** Merging this tag with another. This method is used to apply changes to tags and to get latest version
    */
  def mergeWith(that:TagSource) = {
    val (newer, older) = if(this.timestamp.get.after(that.timestamp.get)) (this, that) else (that, this)
    (older.operation, newer.operation) match {
      case (TagOperation.delete, TagOperation.create) => newer
      case (TagOperation.delete, _) => older
      case (_, TagOperation.addFilter) => {
        older.addFilter(newer.filterValue)
        older
      }
      case (_, TagOperation.removeFilter) => {
        older.removeFilter(newer.filterValue)
        older
      }
      case _ => newer
    }
  }
  /** Add or remove a class to the tag scope. Note that negative values means explusion of classes
    */
  def addFilter(toAdd:Set[Int]):this.type
  /** Indicates wether the tag contains all information necessary for its creation. It woul be false when the opration is anly addFilter
  */
  def removeFilter(toRemove:Set[Int]):this.type

  def isFull =  TagOperation.fullOperations(this.operation)
}

case class TagTree(tag:SomeTagSource, children:Seq[TagTree])
object TagTree {
  def apply(i:Int, nodes:Seq[(TagSource, Seq[Int])], added:Set[Int] = Set[Int]()):TagTree =
  if(added(i)) throw new Exception("Loop found on TagTree")
  else nodes(i) match {case (tag, children) => TagTree(tag.toSomeSource, children.map(j =>TagTree(j, nodes)))}
}

case class SomeTagSource(classifier:Option[ClassifierTagSource], clustering:Option[ClusterTagSource], analogy:Option[AnalogyTagSource]) {
  assert(!classifier.orElse(clustering).orElse(analogy).isEmpty)
  def source:TagSource = classifier.orElse(clustering).orElse(analogy).get
  def resetTimestamp = {source.resetTimestamp;this}
}
object SomeTagSource {
  def apply(source:TagSource):SomeTagSource = source match {
    case a:ClassifierTagSource => SomeTagSource(classifier = Some(a), clustering = None, analogy = None)
    case a:ClusterTagSource => SomeTagSource(classifier = None, clustering = Some(a), analogy = None)
    case a:AnalogyTagSource => SomeTagSource(classifier = None, clustering = None, analogy = Some(a))
    case _ => throw new Exception(s"Unexpected class for ${source}")
  }
}

object TagSource {
  def getTags(ds:Dataset[SomeTagSource]) = {
    import ds.sparkSession.implicits._
    ds.map(a => (a.source.id, a.source.timestamp.get, a))
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

  def getNodeParams(tags:Seq[TagSource]):Seq[NodeParams] =  calculateTree(tags).map{case (tag, children, cPath) => tag.toNodeParams(children, cPath)}
  def getNodeParams(ds:Dataset[SomeTagSource]):Seq[NodeParams] = getNodeParams(getTags(ds))
  def getTagTree(ds:Dataset[SomeTagSource]):TagTree =  getTagTree(getTags(ds))
  def getTagTree(tags:Seq[TagSource]):TagTree =  TagTree(0, calculateTree(tags).map{case (tag, children, cPath) => (tag, children)})


  def calculateTree(
    tags:Seq[TagSource]
    , nodes:ArrayBuffer[TagSource]=ArrayBuffer[TagSource]()
    , leafs:Seq[Int]=Seq[Int]()
    , children:ArrayBuffer[Seq[Int]]= ArrayBuffer[Seq[Int]]()
    , classPath:ArrayBuffer[Map[Int, Set[Int]]]= ArrayBuffer[Map[Int, Set[Int]]]()
  ):Seq[(TagSource, Seq[Int], Map[Int, Set[Int]])] = {
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
          classPath += tag.outClassesFor(0).get.map(oClass => oClass -> Set(0)).toMap
          children += Seq[Int]()
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
                      Some(path.flatMap(pClass => tag.outClassesFor(pClass)).head.map(cOut => (cOut -> path)))
                    else
                      None
                  }
                  .headOption.asInstanceOf[Option[Set[(Int, Set[Int])]]]
              }.asInstanceOf[Set[Set[(Int, Set[Int])]]] match {
                case s if s.filter(paths => paths.size > 0).size == tag.filterValue.filter(_ >= 0).size =>
                  nodes += tag
                  children += Seq[Int]()
                  children(iNode) = children(iNode) :+ (nodes.size - 1)
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
      nodes.zip(children.zip(classPath)).map{case(n, (ch, cPath)) => (n, ch, cPath)}
    } else if(rest.size ==  tags.size) {
      throw new Exception("Cannot obtain node tree representation, cannot match nodes to add as leafs and there are still nodes to be included")
    } else
      calculateTree(tags = rest, nodes = nodes, leafs = newLeafs, children = children, classPath = classPath)
  }
}

case class ClassifierTagSource(
  id:Int
  , operation:TagOperation
  , var timestamp:Option[Timestamp]
  , name: Option[String]
  , color:Option[String]
  , inTag:Option[Int]
  , outTags: Option[Set[Int]]
  , oFilterMode:Option[FilterMode]=None
  , var oFilterValue:Option[Set[Int]]=None
) extends TagSource {
  assert(!TagOperation.fullOperations(this.operation) || (!inTag.isEmpty && !name.isEmpty && !outTags.isEmpty))
  def filterMode = this.oFilterMode.getOrElse(FilterMode.anyIn)
  def filterValue = this.oFilterValue.getOrElse(Set(inTag.get))
  def outClassesFor(iClass:Int):Option[Set[Int]] = if(iClass == inTag.get) Some(outTags.get) else None
  def outClasses() = outTags.get
  def toNodeParams(children:Seq[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name.get
     , color = this.color
     , tagId = Some(this.id)
     , algo = ClassAlgorithm.supervised
     , strLinks = Map(inTag.get.toString -> outTags.get)
     , filterMode = this.filterMode
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , maxTopWords = None
     , classCenters= None
     , childSplitSize = None
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2))
   )
  def addFilter(toAdd:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue ++ toAdd)
    this
  }
  def removeFilter(toRemove:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue -- toRemove)
    this
  }
}
case class AnalogyTagSource(
  id:Int
  , operation:TagOperation
  , var timestamp:Option[Timestamp]
  , name:Option[String]
  , color:Option[String]
  , referenceTag:Option[Int]
  , baseTag:Option[Int]
  , analogyClass:Option[Int]
  , oFilterMode:Option[FilterMode] = None
  , var oFilterValue:Option[Set[Int]] = None
) extends TagSource {
  def filterValue = this.oFilterValue.getOrElse(Set(baseTag.get,referenceTag.get))
  def filterMode = this.oFilterMode.getOrElse(FilterMode.allIn)
  def outClassesFor(iClass:Int) = if(iClass == baseTag.get) Some(Set(analogyClass.get)) else None
  def outClasses() = Set(analogyClass.get)
  def toNodeParams(children:Seq[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name.get
     , color = this.color
     , tagId = Some(this.id)
     , algo = ClassAlgorithm.analogy
     , strLinks = Map(baseTag.get.toString -> Set(analogyClass.get))
     , filterMode = this.filterMode
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , maxTopWords = None
     , classCenters= None
     , childSplitSize = None
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2))
   )
  def addFilter(toAdd:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue ++ toAdd)
    this
  }
  def removeFilter(toRemove:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue -- toRemove)
    this
  }
}
case class ClusterTagSource (
  id:Int
  , operation:TagOperation
  , var timestamp:Option[Timestamp]
  , name:Option[String]
  , color:Option[String]
  , strLinks:Option[Map[String, Set[Int]]]
  , maxTopWords:Option[Int]
  , childSplitSize:Option[Int]
  , oFilterMode:Option[FilterMode] = None
  , var oFilterValue:Option[Set[Int]] = None
) extends TagSource {
  def filterValue = this.oFilterValue.getOrElse(this.strLinks.get.keys.map(_.toInt).toSet)
  def filterMode = this.oFilterMode.getOrElse(FilterMode.allIn)
  def outClassesFor(iClass:Int) = strLinks.get.get(iClass.toString)
  def outClasses() = strLinks.get.values.flatMap(v => v).toSet
  def toNodeParams(children:Seq[Int], classPath:Map[Int, Set[Int]]) =
    NodeParams(
     name = this.name.get
     , color = this.color
     , tagId = Some(this.id)
     , algo = ClassAlgorithm.clustering
     , strLinks = strLinks.get
     , filterMode = this.filterMode
     , filterValue = ArrayBuffer(this.filterValue.toSeq:_*)
     , maxTopWords = Some(this.maxTopWords.get)
     , classCenters =
         Some(Map(
           strLinks.get
             .flatMap{case (in, outs) => outs.zipWithIndex}
             .groupBy{case(out,center) => out.toString}
             .mapValues(p => p.head._2)
             .toSeq :_*
           ))
     , childSplitSize = Some(this.childSplitSize.get)
     , annotations = ArrayBuffer[Annotation]()
     , hits = 0.0
     , children = ArrayBuffer(children:_*)
     , strClassPath = classPath.map(p => (p._1.toString, p._2))
   )
  def addFilter(toAdd:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue ++ toAdd)
    this
  }
  def removeFilter(toRemove:Set[Int]) = {
    this.oFilterValue =  Some(this.filterValue -- toRemove)
    this
  }
}

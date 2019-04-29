package demy.mllib.topic

import demy.util.{log => l}
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.{Vector => MLVector, Vectors}
import org.apache.spark.sql.{SparkSession}
import org.apache.commons.io.IOUtils
import scala.collection.mutable.{ArrayBuffer, HashSet, HashMap}
import scala.{Iterator => It}
import java.io.{ByteArrayOutputStream, ObjectOutputStream}
import java.io.{ObjectInputStream,ByteArrayInputStream}

case class NodeParams(
  inClasses:Set[Int] = Set[Int]()
  , outClasses:Set[Int] = Set[Int]()
  , links:Map[Int, Set[Int]] = Map[Int, Set[Int]]()
  , var filterMode:FilterMode = FilterMode.noFilter
  , filterValue:ArrayBuffer[Int] = ArrayBuffer[Int]()) {
  def getOutMap(init:Double) =  HashMap(outClasses.toSeq.map(v => (v, init)):_*) 
}
case class ClassAlgorithm(value:String)
object ClassAlgorithm {
  val analogy = ClassAlgorithm("analogy")
  val supervised= ClassAlgorithm("classifier")
  val clustering = ClassAlgorithm("clustering")
}
case class FilterMode(value:String)
object FilterMode {
  val noFilter = FilterMode("noFilter")
  val allIn = FilterMode("allIn")
  val anyIn = FilterMode("anyIn")
}

trait Node{
  val name:String
  val tokens:ArrayBuffer[String]
  val points:ArrayBuffer[MLVector]
  val pClasses:ArrayBuffer[Int]
  val children:ArrayBuffer[Node]
  val algo:ClassAlgorithm
  val params:NodeParams
  var hits = 0L
  var stepHits = 0L

  def walk(vClasses:Array[Int], scores:Option[Array[Double]], dag:Option[Array[Int]] 
      , vectors:Seq[MLVector], tokens:Seq[String], spark:SparkSession
      , wClasses:ArrayBuffer[(Int, Int, Double)] = ArrayBuffer[(Int, Int, Double)]()
      , maxDeep:Option[Int] = None ) {
    this.stepHits = this.stepHits + 1
    transform(vClasses, scores, dag, vectors, tokens, spark)
    wClasses ++= 
      (for(i <- It.range(0, vClasses.size) if this.params.outClasses(vClasses(i))) 
        yield (i, vClasses(i),scores match {case Some(s) =>s(i) case _ => 1.0} )) 
    for(i <- It.range(0, this.children.size) if maxDeep.getOrElse(1)>0) {
      if(this.children(i).params.filterMode == FilterMode.noFilter
        || this.children(i).params.filterMode == FilterMode.allIn 
           &&  vClasses
                .iterator
                .flatMap(v => if (this.children(i).params.inClasses(v)) Some(v) else None)
                .toSeq.distinct.size 
              == this.children(i).params.inClasses.size 
        || this.children(i).params.filterMode == FilterMode.anyIn 
           &&  vClasses
                .iterator
                .flatMap(v => if (this.children(i).params.inClasses(v)) Some(v) else None)
                .toSeq.distinct.size 
              > 0
      )
      this.children(i).walk(vClasses, scores, dag, vectors, tokens, spark, wClasses, maxDeep.map(d => d - 1))
    }
  }
  def transform(vClasses:Array[Int], scores:Option[Array[Double]], dag:Option[Array[Int]] 
    , vectors:Seq[MLVector], tokens:Seq[String], spark:SparkSession)

  def setToken(token:String, pClass:Int):this.type = 
    tokens.indexOf(token) match {
      case -1 => throw new Exception("Cannot add new token without a vector")
      case j => this.setToken(token = token, point = this.points(j), pClass = pClass) 
  }
  def setToken(token:String, point:MLVector, pClass:Int):this.type = {
    tokens.indexOf(token) match {
      case -1 =>
        this.tokens += token
        this.points += point
        this.pClasses += pClass
      case j => 
        this.points(j) = point
        this.pClasses(j) = pClass
    }
    this
  }

  def clusteringGAP:Double = {
    this match {
      case n:ClusteringNode => n.leafsGAP
      case n if n.children.size > 0 => n.children.map(c => c.clusteringGAP).sum
      case _ => 0.0
    }
  }
  def nodesIterator:Iterator[Node] = {
    It(this) ++ (for(i <- It.range(0, this.children.size)) yield this.children(i).nodesIterator).reduceOption(_ ++ _).getOrElse(It[Node]())
  }
  def similarityScore(vectors:Seq[MLVector], vClasses:Seq[Int]):Double = {
    val bestScores = this.params.getOutMap(0.0)
    for{
      i <- It.range(0, vectors.size) 
      j <- It.range(0, this.points.size)
      if vClasses(i) == this.pClasses(j)
    } {
      vectors(i).cosineSimilarity(points(j)) match {
        case sim if sim > bestScores(vClasses(i)) =>
          bestScores(vClasses(i)) = sim
        case _ =>
      }
    }
    bestScores.values.reduce(_ * _)
  }

  def encode(childArray:ArrayBuffer[EncodedNode]):Int= {
    val encoder = EncodedNode(name = this.name, algo = this.algo, tokens = tokens, points = points, pClasses = pClasses, params = params, hits = hits, stepHits = stepHits)
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
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> name: ${name}\n")
    buffer += (s"${Range(0, level).map(_ => "-").mkString}> algo: ${algo}\n")
    prettyPrintExtras(level = level, buffer = buffer, stopLevel = stopLevel)
    if(stopLevel == -1 || level <= stopLevel)
      this.children.foreach(c => c.prettyPrint(level = level + 1, buffer = buffer, stopLevel = stopLevel))
    buffer
  }
  def prettyPrintExtras(level:Int = 0, buffer:ArrayBuffer[String]=ArrayBuffer[String](), stopLevel:Int = -1):ArrayBuffer[String]
  def encodeExtras(encoder:EncodedNode)
}

case class EncodedNode  (
  name:String
  , algo:ClassAlgorithm
  , tokens:ArrayBuffer[String] = ArrayBuffer[String]()
  , points:ArrayBuffer[MLVector] = ArrayBuffer[MLVector]()
  , pClasses:ArrayBuffer[Int] = ArrayBuffer[Int]()
  , params:NodeParams
  , children: ArrayBuffer[Int] = ArrayBuffer[Int]()
  , var hits:Long = 0
  , var stepHits:Long = 0
  , var referenceClass:Int = 0
  , var inAnalogy:ArrayBuffer[Boolean] = ArrayBuffer[Boolean]()
  , var classCenters:Map[Int, Int] = Map[Int, Int]()
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
      if(this.algo == ClassAlgorithm.clustering) 
        ClusteringNode(this)
      else if(this.algo == ClassAlgorithm.supervised)
        ClassifierNode(this)
      else if(this.algo == ClassAlgorithm.analogy)
        AnalogyNode(this)
      else throw new Exception(s"Unknown algorithm ${this.algo}")
    n.children ++= this.children.map(i => others(i).decode(others))
    n
  }

  def stripBinary = {
    EncodedNode  (
      name = name
      , algo = algo
      , tokens = tokens
      , points = points
      , pClasses = pClasses
      , params = params
      , children = children
      , hits = hits
      , stepHits = stepHits
      , referenceClass = referenceClass
      , inAnalogy = inAnalogy
      , classCenters = classCenters
      , serialized = ArrayBuffer[(String, Array[Byte])]()
    )
  }

  def merge(that:EncodedNode) = {
    EncodedNode(
      name = name
      , algo = this.algo
      , tokens = this.tokens
      , points = this.points
      , pClasses = this.pClasses
      , params = this.params
      , children = this.children
      , hits = this.hits
      , stepHits = this.stepHits + that.stepHits
      , referenceClass = this.referenceClass
      , inAnalogy = this.inAnalogy
      , classCenters = this.classCenters
      , serialized = this.serialized
    )
  
  }
  def addStepHits{
    this.hits = this.hits + this.stepHits
    this.stepHits = 0
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

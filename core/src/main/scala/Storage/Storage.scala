package demy.storage

import demy.util.log
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, Path => HPath}
import org.apache.hadoop.conf.Configuration
import org.apache.commons.lang.RandomStringUtils
import org.apache.commons.io.IOUtils

import java.io.InputStream
import java.util.ArrayDeque
import java.nio.file.{Files, Paths, Path => LPath}
import java.nio.file.StandardCopyOption

import scala.collection.JavaConverters._

trait FSNode {
  val path:String
  val storage:Storage
  val isLocal:Boolean 
  val sparkCanRead:Boolean 
  val attrs:Map[String, String]

  def exists = storage.exists(this)
  def isDirectory = storage.isDirectory(this)
  def delete(recurse:Boolean = false) = storage.delete(this, recurse) 
  def getContent = storage.getContent(this)
  def getContentAsString = storage.getContentAsString(this)
  def getContentAsJson = storage.getContentAsJson(this)
  def setContent(content:InputStream) = {storage.setContent(this, content);this}
}

case class LocalNode(path:String, storage:LocalStorage, sparkCanRead:Boolean, attrs:Map[String, String]= Map[String, String]()) extends FSNode{
  val isLocal:Boolean = true
  val jPath = Paths.get(path) 

} 
case class EpiFileNode(path:String, storage:EpiFileStorage, attrs:Map[String, String]= Map[String, String]()) extends FSNode{
  val isLocal = false
  val sparkCanRead = false
  def setPath(path:String) =  EpiFileNode(path=path, storage=this.storage, attrs=this.attrs)
}

case class HDFSNode(path:String, storage:HDFSStorage, attrs:Map[String, String]= Map[String, String]()) extends FSNode {
  val isLocal = false
  val sparkCanRead = true

  val hPath = new HPath(path)
}

trait Storage {
  val protocol:String  
  val tmpDir:String 
  val sparkCanRead:Boolean 
  val tmpPrefix:Option[String]=None 
  val linkedStorage:Option[Storage]=None
  def exists(node:FSNode):Boolean
  def isDirectory(node:FSNode):Boolean 
  def delete(node:FSNode, recurse:Boolean = false) 
  def getContent(node:FSNode):InputStream
  def getContentAsString(node:FSNode) = {
    val s = new java.util.Scanner(this.getContent(node)).useDelimiter("\\A") 
    if(s.hasNext())  s.next()
    else "";
  }
  def getContentAsJson(node:FSNode) = scala.util.parsing.json.JSON.parseFull(getContentAsString(node))
  
  def setContent(node:FSNode, data:InputStream)
  def last(path:Option[String], attrPattern:Map[String, String]):Option[FSNode]
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode

  val tmpFiles:ArrayDeque[FSNode]= new ArrayDeque[FSNode]()
  def removeMarkedFiles(removeTmpDir:Boolean=false) {
    while(tmpFiles.size > 0) {
      tmpFiles.pop().delete()
    }
    if(removeTmpDir && this.tmpDir!=null)
      this.getNode(path = this.tmpDir).delete(recurse = true)
    linkedStorage match {case Some(s) => s.removeMarkedFiles() case _ => {}}
  } 
  def getTmpPath(fixName:Option[String]=None):String

  def getTmpNode(fixName:Option[String]=None) = {
    ensurePathExists(tmpDir)
    getNode(path = getTmpPath(fixName)) 
  }
  def markForRemoval(n:FSNode) {
    tmpFiles.push(n)
  }
  def ensurePathExists(path:String)
}

object Storage {
  def getSparkStorage(spark:SparkSession) = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    if(hadoopConf == null)
      LocalStorage(sparkCanRead = true)
    else
      HDFSStorage(hadoopConf = hadoopConf)
  }
}

case class LocalStorage(override val sparkCanRead:Boolean=false, override val tmpPrefix:Option[String]=None) extends Storage {
  override val protocol:String="file"  
  val tmpDir = {
    ((System.getenv("TMPDIR") match {case null => System.getProperty("java.io.tmpdir") case s => s}) + "/" 
     + (tmpPrefix match {case Some(s) => s"${s}_" case _ => ""})
     + RandomStringUtils.randomAlphanumeric(10))
  }

  def exists(node:FSNode) 
       = node match { case lNode:LocalNode => Files.exists(lNode.jPath)          case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def isDirectory(node:FSNode) 
       = node match { case lNode:LocalNode => Files.isDirectory(lNode.jPath)     case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def delete(node:FSNode, recurse:Boolean = false) 
       = node match { case lNode:LocalNode => {
         if(Files.isDirectory(lNode.jPath) && recurse ) {
           Files.list(lNode.jPath).iterator().asScala.foreach{ cPath =>
             LocalNode(path = cPath.toAbsolutePath().toString(), storage=lNode.storage, sparkCanRead = this.sparkCanRead).delete()
           }
         }
       } case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def getContent(node:FSNode)
       = node match { case lNode:LocalNode => Files.newInputStream(lNode.jPath)               case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def setContent(node:FSNode, data:InputStream)
       = node match { case lNode:LocalNode => {Files.copy(data, lNode.jPath, StandardCopyOption.REPLACE_EXISTING)} 
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(path:Option[String], attrPattern:Map[String, String] = Map[String, String]())  = throw new Exception("Not Implemented")
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = LocalNode(path = path, storage = this, attrs=attrs, sparkCanRead=this.sparkCanRead)
  def getTmpPath(fixName:Option[String]=None) 
       = tmpDir+"/"+ (fixName match {case Some(name) => name case _ =>  RandomStringUtils.randomAlphanumeric(10)})
  def ensurePathExists(path:String) = Files.createDirectories(Paths.get(path))
}

case class EpiFileStorage(vooUrl:String, user:String, pwd:String) extends Storage {
  override val protocol:String="epi" 
  val localFS:LocalStorage=LocalStorage()
  override val linkedStorage = Some(localFS)
  override val sparkCanRead = false
  val tmpDir = null 
  def exists(node:FSNode) 
       = node match { case eNode:EpiFileNode => EpiFiles.exists(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd)          
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def isDirectory(node:FSNode) = false
  def delete(node:FSNode, recurse:Boolean = false) { throw new Exception("Not implemented") }
  def getContent(node:FSNode)
       = node match { case eNode:EpiFileNode => { EpiFiles.download(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd) }
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def setContent(node:FSNode, data:InputStream)
       = node match { case eNode:EpiFileNode => {
                        val tmp = localFS.getTmpNode().setContent(data)
                        val id = EpiFiles.epifileUpload(vooUrl=this.vooUrl, user=this.user, pwd=this.pwd, path=tmp.path, name=node.attrs("name"), comment=node.attrs.get("comment").getOrElse(""))
                        tmp.delete()
                        eNode.setPath(id.get) 
                      }
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(path:Option[String], attrPattern:Map[String, String] = Map[String, String]()) = {
     val commentPattern = attrPattern.get("comment")
     val namePattern = attrPattern.get("name")

     EpiFiles.findFile(commentPattern = commentPattern, namePattern=namePattern, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd) match {
       case Some((name, comment, id, date)) => Some(EpiFileNode(path = id, storage=this, attrs = Map[String, String]("comment"->comment, "name"->name, "date"->date, "id"->id)))
       case _ => None
     }
  }
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = EpiFileNode(path = path, storage = this, attrs=attrs)
  def getTmpPath(fixName:Option[String]=None):String = {throw new Exception("Not implemented")}
  def ensurePathExists(path:String) = throw new Exception("Not implemented")
}
case class HDFSStorage(hadoopConf:Configuration, override val tmpPrefix:Option[String]=None) extends Storage {
  override val protocol:String="hdfs"
  override val sparkCanRead = true
  val localFS:LocalStorage = LocalStorage()
  override val linkedStorage = Some(localFS)
  val fs = FileSystem.get(hadoopConf)
  val tmpDir = {
    (hadoopConf.get("hadoop.tmp.dir")+ "/" 
     + (tmpPrefix match {case Some(s) => s"${s}_" case _ => ""}) 
     + RandomStringUtils.randomAlphanumeric(10))
  }
  def exists(node:FSNode) 
       = node match { case hNode:HDFSNode => fs.exists(hNode.hPath)          case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def isDirectory(node:FSNode) 
       = node match { case hNode:HDFSNode => fs.isDirectory(hNode.hPath)     case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def delete(node:FSNode, recurse:Boolean = false) 
       = node match { case hNode:HDFSNode => fs.delete(hNode.hPath, recurse) case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def getContent(node:FSNode)
       = node match { case hNode:HDFSNode => fs.open(hNode.hPath)            case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def setContent(node:FSNode, data:InputStream) 
       = node match { case hNode:HDFSNode => {
                        val writer = fs.create(hNode.hPath, true)
                        IOUtils.copy(data,writer)
                        writer.close()
                      } 
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(path:Option[String], attrPattern:Map[String, String] =  Map[String, String]())  = {
    val it = fs.listFiles(new HPath(path.getOrElse("/")), true)
    var lastModified = Long.MinValue
    var lastFile:Option[FSNode] = None 
    while (it.hasNext()) {
      val file = it.next()
      val filePath = file.getPath()
      val fileName = filePath.getName()
      val isMatch = attrPattern.get("name") match { case Some(pattern) => !pattern.r.findFirstIn(fileName).isEmpty case _ => true}
      val modified = file.getModificationTime()
      lastFile = if(isMatch && modified > lastModified) {
        lastModified = modified
        Some(this.getNode(path = filePath.toString))
      } else {lastFile}
    }
    lastFile
  }
  def getNode(path:String, attrs:Map[String, String]=Map[String, String]()):FSNode
       = HDFSNode(path = path, storage = this, attrs=attrs)
  def getTmpPath(fixName:Option[String]=None)
       = tmpDir+"/"+ (fixName match {case Some(name) => name case _ =>  RandomStringUtils.randomAlphanumeric(10)})
  def ensurePathExists(path:String) = fs.mkdirs(new HPath(path))
 
}

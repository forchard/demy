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
  val isTemporary:Boolean 
  val sparkCanRead:Boolean 
  val attr:Map[String, String]

  def exists = storage.exists(this)
  def isDirectory = storage.isDirectory(this)
  def delete(recurse:Boolean = false) = storage.delete(this, recurse) 
  def getStream = storage.getStream(this)
}

case class LocalNode(path:String, storage:LocalStorage, isTemporary:Boolean, sparkCanRead:Boolean, attr:Map[String, String]= Map[String, String]()) extends FSNode{
  val isLocal:Boolean = true
  val jPath = Paths.get(path) 

} 
case class EpiFileNode(path:String, storage:EpiFileStorage, isTemporary:Boolean, attr:Map[String, String]= Map[String, String]()) extends FSNode{
  val isLocal = false
  val sparkCanRead = false 
}

case class HDFSNode(path:String, storage:HDFSStorage, isTemporary:Boolean, attr:Map[String, String]= Map[String, String]()) extends FSNode {
  val isLocal = false
  val sparkCanRead = true

  val hPath = new HPath(path)
}

trait Storage {
  val protocol:String  
  val tmpFiles:ArrayDeque[FSNode]= new ArrayDeque[FSNode]()
  val tmpPrefix:Option[String]=None 
  val tmpDir:String 
  def exists(node:FSNode):Boolean
  def isDirectory(node:FSNode):Boolean 
  def delete(node:FSNode, recurse:Boolean = false) 
  def getContent(node:FSNode):InputStream
  def setContent(node:FSNode, data:InputStream)
  def last(namePattern:Option[String], attrPattern:Map[String, String]):Option[FSNode]
  def removeTmpFiles() {
    while(tmpFiles.size > 0) {
      tmpFiles.pop().delete()
    }
  } 
  def getTmpPath(get):String
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

case class LocalStorage(sparkCanRead:Boolean=false, tmpPrefix:Option[String]=None) extends Storage {
  val protocol:String="file"  
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
             LocalNode(path = cPath.toAbsolutePath().toString(), storage=lNode.storage, isTemporary = lNode.isTemporary, sparkCanRead = this.sparkCanRead).delete()
           }
         }
       } case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def getContent(node:FSNode)
       = node match { case lNode:LocalNode => Files.newInputStream(lNode.jPath)               case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def setContent(node:FSNode, data:InputStream)
       = node match { case lNode:LocalNode => Files.copy(data, lNode.jPath, StandardCopyOption.REPLACE_EXISTING) 
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(namePattern:Option[String], attrPattern:Map[String, String] = Map[String, String]())  = throw new Exception("Not Implemented")
}

case class EpiFileStorage(protocol:String="epi", vooUrl:String, user:String, pwd:String) extends Storage {
  val localFS:LocalStorage=LocalStorage() 
  val tmpDir = null 
  def exists(node:FSNode) 
       = node match { case eNode:EpiFileNode => EpiFiles.exists(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd)          
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def isDirectory(node:FSNode) = false
  def delete(node:FSNode, recurse:Boolean = false) { throw new Exception("Not implemented") }
  def getContent(node:FSNode)
       = node match { case eNode:EpiFileNode => EpiFiles.download(id = eNode.path, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd)          
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")
       }
  def setContent(node:FSNode, data:InputStream)
       = node match { case eNode:EpiFileNode => {
                        localFS.
                        IOUtils.copy(data, fs.create(f = hNode.hPath, overwrite = true)) 
                      }
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(namePattern:Option[String], attrPattern:Map[String, String] = Map[String, String]()) = {
     val commentPattern = if(attrPattern.size == 0) None
                          else if(attrPattern.size == 1 && attrPattern.contains("comment")) attrPattern.get("comment")
                          else throw new Exception("EpiFile Storage currently supports listing files only by name and comment")
     EpiFiles.findFile(commentPattern = commentPattern, namePattern=namePattern, vooUrl = this.vooUrl, user = this.user, pwd = this.pwd) match {
       case Some((name, comment, id, date)) => Some(EpiFileNode(path = id, storage=this, isTemporary = false, attr = Map[String, String]("comment"->comment, "name"->name, "date"->date, "id"->id)))
       case _ => None
     }
  }
}
case class HDFSStorage(protocol:String="hdfs", hadoopConf:Configuration, tmpPrefix:Option[String]=None) extends Storage {
  val localFS:LocalStorage = LocalStorage(tmpForced=this.tmpDir)
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
       = node match { case hNode:HDFSNode => IOUtils.copy(data, fs.create(f = hNode.hPath, overwrite = true)) 
                      case _ => throw new Exception(s"HDFS Storage cannot manage ${node.getClass.getName} nodes")}
  def last(namePattern:Option[String], attrPattern:Map[String, String] =  Map[String, String]())  = throw new Exception("Not Implemented")
 
}

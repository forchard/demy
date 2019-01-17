package demy.mllib.index;
import org.apache.lucene.index.{DirectoryReader}
import org.apache.lucene.search.{IndexSearcher}
import demy.storage.{Storage, LocalNode}

case class DefaultIndexStrategy(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader, usePopularity:Boolean = false) extends IndexStrategy {
  def this() = this(null, null, null, false)
  def getReadStrategy() = StandardStrategy(searcher=searcher, indexDirectory=indexDirectory, reader=reader, usePopularity= usePopularity)
  def setProperty(name:String,value:String) = {
    name match {
      case "usePopularity" => DefaultIndexStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader, usePopularity = value.toBoolean)
      case _ => throw new Exception(s"Not supported property ${name} on DefaultIndexStrategy")
    }
  }
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader)
        =  DefaultIndexStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader)

}

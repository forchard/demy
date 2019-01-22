package demy.mllib.index;
import org.apache.lucene.index.{DirectoryReader}
import org.apache.lucene.search.{IndexSearcher}
import demy.storage.{Storage, LocalNode}

case class NgramStrategy(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader, usePopularity:Boolean = false, Nngrams:Int= 3 ) extends IndexStrategy {
  def this() = this(null, null, null, false)
  def getReadStrategy() = NgramReadStrategy(searcher=searcher, indexDirectory=indexDirectory, reader=reader, usePopularity= usePopularity, Nngrams=Nngrams)
  def setProperty(name:String,value:String) = {
    name match {
      case "Nngrams" => NgramStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader, usePopularity = usePopularity, Nngrams=value.toInt)
      case "usePopularity" => NgramStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader, usePopularity = value.toBoolean, Nngrams=Nngrams)
      case _ => throw new Exception(s"Not supported property ${name} on NgramReadStrategy")
    }
  }
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader)
        =  NgramStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader)

}

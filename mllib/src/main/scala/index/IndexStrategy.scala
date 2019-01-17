package demy.mllib.index;

import org.apache.lucene.document.Document
import org.apache.lucene.index.{DirectoryReader}
import org.apache.lucene.search.{IndexSearcher}
import demy.storage.{Storage, LocalNode}

trait IndexStrategy {
  val searcher:IndexSearcher
  val indexDirectory:LocalNode
  val reader:DirectoryReader

  def getReadStrategy() : IndexReaderStrategy
  def setProperty(name:String, value:String) : IndexStrategy
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader) : IndexStrategy

}

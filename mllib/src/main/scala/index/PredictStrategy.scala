package demy.mllib.index;

import org.apache.lucene.search.{IndexSearcher, TermQuery, BooleanQuery, FuzzyQuery}
import org.apache.lucene.store.NIOFSDirectory
import org.apache.lucene.index.{DirectoryReader, Term}
import org.apache.lucene.search.BooleanClause.Occur
import org.apache.lucene.queries.function.FunctionQuery
import org.apache.lucene.queries.function.valuesource.DoubleFieldSource
import org.apache.lucene.search.BoostQuery
import org.apache.lucene.document.Document
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.lucene.document.{Document, TextField, StringField, IntPoint, BinaryPoint, LongPoint, DoublePoint, FloatPoint, Field, StoredField}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io.{ObjectInputStream,ByteArrayInputStream}
import scala.collection.JavaConverters._
import demy.storage.LocalNode
import demy.util.log
import demy.util.implicits.IterableUtil

case class PredictStrategy(searcher:IndexSearcher, indexDirectory:LocalNode, reader:DirectoryReader) extends IndexStrategy {
  def this() = this(null, null, null)
  def setProperty(name:String,value:String) = {
    name match {
      case _ => throw new Exception(s"Not supported property ${name} on NgramReadStrategy")
    }
  }
  def set(searcher:IndexSearcher, indexDirectory:LocalNode,reader:DirectoryReader) =  PredictStrategy(searcher = searcher, indexDirectory = indexDirectory,reader = reader)


override def searchDoc(
  terms:Array[String]
  , maxHits:Int
  , maxLevDistance:Int=2
  , filter:Row = Row.empty 
  , usePopularity:Boolean
  , minScore:Double=0.0
  , boostAcronyms:Boolean=false
  , termWeights:Option[Seq[Double]]=None
  , caseInsensitive:Boolean = true
  , tokenize:Boolean = true 
  ) = {
    val start = 0
    termWeights match {
      case None =>   
        evaluate(
          terms = terms
          , likelihood = terms.map(_ => 1.0)
          , from = 0
          , to = terms.size
          , maxHits = maxHits
          , maxLevDistance = maxLevDistance
          , filter = filter
          , usePopularity = usePopularity
          , minScore = minScore
          , boostAcronyms = boostAcronyms
          , caseInsensitive = caseInsensitive
        )
      case Some(weights) => 
        //We have to find the best prediction to match 
        //finding the center of the highest 4 terms avg
        var imax = 0
        var v = 0.0
        var vmax = 0.0
        for(i <- Iterator.range(0, weights.size)) {
          v = v + weights(i)
          if(i > 3) v = v - weights(i - 3)
          if(v > vmax) {
            imax = i
            vmax = v  
          }
        }
        var ifrom = imax
        var ito = imax
        v = weights(imax)
        //Expanding the range until for closer likelihood bigger than 0.75 the found center
        for(i <- Iterator.range(1, 3)) {
          if(imax - i >= 0 && weights(imax - i)> 0.75*v) {
            ifrom = i
          }
          if(imax + i < weights.size && weights(imax + i) > 0.75*v) {
            ito = i
          }
        }
        evaluate(
          terms = terms
          , likelihood = weights
          , from = ifrom
          , to = ito + 1
          , maxHits = maxHits
          , maxLevDistance = maxLevDistance
          , filter = filter
          , usePopularity = usePopularity
          , minScore = minScore
          , boostAcronyms = boostAcronyms
          , caseInsensitive = caseInsensitive
        )
    }

  }

}

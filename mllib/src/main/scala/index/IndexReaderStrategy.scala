package demy.mllib.index;

import org.apache.lucene.search.IndexSearcher
import org.apache.lucene.document.Document
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import java.io.{ObjectInputStream,ByteArrayInputStream}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.lucene.search.ScoreDoc
import org.apache.lucene.index.{DirectoryReader}
import org.apache.lucene.search.{IndexSearcher}
import demy.storage.{Storage, LocalNode}



trait IndexReaderStrategy {
  val reader:DirectoryReader
  val indexDirectory:LocalNode
  val searcher:IndexSearcher



  def searchDoc(query:String, maxHits:Int, filter:Row, maxLevDistance:Int,
                minScore:Double, boostAcronyms:Boolean ):Array[SearchMatch]

  def search(query:String, maxHits:Int, filter:Row = Row.empty, outFields:Seq[StructField]=Seq[StructField](),
             maxLevDistance:Int=2 , minScore:Double=0.0, boostAcronyms:Boolean=false, showTags:Boolean=false):Array[GenericRowWithSchema] = {


    // return (doc, score) Array[ScoreDoc]
    val hits = searchDoc(query, maxHits, filter, maxLevDistance, minScore, boostAcronyms)

    val outSchema = StructType(outFields.toList :+ StructField("_score_", FloatType)
                                                :+ StructField("_tags_", ArrayType(StringType))
                                                :+ StructField("_startIndex_", IntegerType)
                                                :+ StructField("_endIndex_", IntegerType))
                                              //  :+ StructField("_pos_", ArrayType(IntegerType, IntegerType)) ) // add fields

    if (query != null) {
      hits.flatMap(hit => {
        if(hit.score < minScore) None
        else {
          val doc = searcher.doc(hit.docId)
          Some(new GenericRowWithSchema(
            values = outFields.toArray.map(field => {
              val lucField = doc.getField(field.name)
              if(field.name == null || lucField == null) null
              else
                field.dataType match {
              case dt:StringType => lucField.stringValue
              case dt:IntegerType => lucField.numericValue().intValue()
              case dt:BooleanType => lucField.binaryValue().bytes(0) == 1.toByte
              case dt:LongType =>  lucField.numericValue().longValue()
              case dt:FloatType => lucField.numericValue().floatValue()
              case dt:DoubleType => lucField.numericValue().doubleValue()
              case dt => {
                var obj:Any = null
                val serData= lucField.binaryValue().bytes;
                if (serData!=null) {
                   val in=new ObjectInputStream(new ByteArrayInputStream(serData))
                   obj = in.readObject()
                   in.close()
                }
                obj
              }
            }}) ++ Array(hit.score, hit.ngram.text, hit.ngram.startIndex, hit.ngram.endIndex)// add fields
            ,schema = outSchema))
        }
      })
    } else Array[GenericRowWithSchema]()
  }

  def close(deleteSnapShot:Boolean = false) {
    reader.close
    reader.directory().close
    if(deleteSnapShot && indexDirectory.exists) {
      indexDirectory.deleteIfTemporary(recurse = true)
    }
  }


  def deleteRecurse(path:String) {
      if(path!=null && path.length>1 && path.startsWith("/")) {
          val f = java.nio.file.Paths.get(path).toFile
          if(!f.isDirectory)
            f.delete
          else {
              f.listFiles.filter(ff => ff.toString.size > path.size).foreach(s => this.deleteRecurse(s.toString))
              f.delete
          }
      }
  }


}

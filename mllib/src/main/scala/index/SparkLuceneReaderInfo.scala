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

//<<<<<<< HEAD
//case class SparkLuceneReaderInfo(searcher:IndexSearcher, indexDirectory:LocalNode, reader:DirectoryReader, usePopularity:Boolean = false) {
//    def search(query:String, maxHits:Int, filter:Row = Row.empty, outFields:Seq[StructField]=Seq[StructField](), maxLevDistance:Int=2 , minScore:Double=0.0, boostAcronyms:Boolean=false) = {
//=======

case class SparkLuceneReaderInfo(searcher:IndexSearcher, indexDirectory:LocalNode, reader:DirectoryReader, usePopularity:Boolean = false) {


  def score_terms(terms:Array[String], query:String, maxLevDistance:Int, filter:Row, outFields:Seq[StructField], maxHits:Int): (Array[String], Float, Document, String, StructType) = {
    val qb = new BooleanQuery.Builder() //  combines multiple TermQuery instances into a BooleanQuery with multiple BooleanClauses, where each clause contains a sub-query and operator
    val fuzzyb = new BooleanQuery.Builder()

    if(maxLevDistance>0) {
        terms.foreach(s => {
            var allLetterUppercase = Range(0, s.length).forall(ind => s(ind).isUpper)

            // if term is only in Uppercase -> double term: "TX" -> "TXTX" (ensures that term is not neglected due to too less letters)
            if (allLetterUppercase) {
                fuzzyb.add(new BoostQuery(new TermQuery(new Term("_text_", s+s)), 15.00F), Occur.SHOULD) // High boosting factor to find doubles
                fuzzyb.add(new BoostQuery(new TermQuery(new Term("_text_", s.toLowerCase)), 4.00F), Occur.SHOULD)
            } else {
                fuzzyb.add(new FuzzyQuery(new Term("_text_", s.toLowerCase), 1, maxLevDistance), Occur.SHOULD)
                fuzzyb.add(new BoostQuery(new TermQuery(new Term("_text_", s.toLowerCase)), 4.00F), Occur.SHOULD)
            }
        })
    }
    else {
        terms.foreach(s => {
            var allLetterUppercase = Range(0, s.length).forall(ind => s(ind).isUpper)

            // if term is only in Uppercase -> double term: "TX" -> "TXTX" (ensures that term is not neglected due to too less letters)
            if (allLetterUppercase) {
                val bst = new BoostQuery(new TermQuery(new Term("_text_", s+s)), 4.00F)  // Boosting factor of 4.0 for exact match
                fuzzyb.add(bst, Occur.SHOULD)
            } else {
                val bst = new BoostQuery(new TermQuery(new Term("_text_", s.toLowerCase)), 4.00F) // boosting factor of 4.0 for exact match
                fuzzyb.add(bst, Occur.SHOULD)
            }
        })
    }

    qb.add(fuzzyb.build, Occur.MUST)
    if(filter.schema != null) {
       filter.schema.fields.zipWithIndex.foreach(p => p match { case (field, i) =>
          if(!filter.isNullAt(i)) field.dataType match {
          case dt:StringType => qb.add(new TermQuery(new Term(field.name, filter.getAs[String](i))), Occur.MUST)
          case dt:IntegerType => qb.add(IntPoint.newExactQuery("_point_"+field.name, filter.getAs[Int](i)), Occur.MUST)
          case dt:BooleanType => qb.add(BinaryPoint.newExactQuery("_point_"+field.name, Array(filter.getAs[Boolean](i) match {case true => 1.toByte case _ => 0.toByte})), Occur.MUST)
          case dt:LongType => qb.add(LongPoint.newExactQuery("_point_"+field.name, filter.getAs[Long](i)), Occur.MUST)
          case dt:FloatType => qb.add(FloatPoint.newExactQuery("_point_"+field.name, filter.getAs[Float](i)), Occur.MUST)
          case dt:DoubleType => qb.add(DoublePoint.newExactQuery("_point_"+field.name, filter.getAs[Double](i)), Occur.MUST)
          case dt => throw new Exception(s"Spark type {$dt.typeName} cannot be used as a filter since it has not been indexed")
       }})
    }
    val q = if(usePopularity) {
               val pop = new FunctionQuery(new DoubleFieldSource("_pop_"));
               new org.apache.lucene.queries.CustomScoreQuery(qb.build, pop);
            } else qb.build

    //query.replaceAll("[^\\p{L}\\-]+", ",").split(",").foreach(s => qb.add(new TermQuery(new Term("text", s)), Occur.SHOULD))
    val outSchema = StructType(outFields.toList :+ StructField("_score_", FloatType))
    val docs = searcher.search(q, maxHits);
    val hits = docs.scoreDocs;


    if(hits.size>0) {
    val res = hits.map(hit => {
            val doc = searcher.doc(hit.doc)
            (s"\nScore: ${hit.score}\n------------"+doc//.getFields().toList.map(f => s"${f.name}->${f.stringValue() match {case s => s.slice(0, 100)}}").mkString("\n------------")//+"\n Explain: \n"+rInfo.searcher.explain(qb.build, hit.doc)
            , hit.score, doc)
        })//.reduce( (x,y) => { (x._1+"\n----"+y._1, if (x._2 > y._2) x._2 else y._2  )} )
        .reduce( (x,y) => (x, y) match {case ((info1, score1, doc1),(info2, score2, doc2)) =>
                                     if(score1 > score2) (info1, score1, doc1) else (info2, score2, doc2) })

    (terms, res._2, res._3, s"\n Searching: $query \n----"+"\n Tokens: ["+terms.mkString(",")+"]\n ----"+res._1+" \nToken:"+terms.map(s => s"${s}").mkString(",")+"\n query:"+q.toString()+"\n", outSchema)
    }
    else (terms,0.0f, null,null, outSchema)
  }


  def getNgram(terms:Array[String], nTerms:Int, Nngrams:Int, maxScoreLocalIndexFirst:Int, direction:String="left"): Array[String] = {
      // get ngram on left side
      if (direction == "left") {
          // if ngram is on left border of term array
          if ( (maxScoreLocalIndexFirst > 0) & (maxScoreLocalIndexFirst+Nngrams < nTerms) )  terms.slice(maxScoreLocalIndexFirst-1, maxScoreLocalIndexFirst+Nngrams)
          // left border
          else if (maxScoreLocalIndexFirst == 0)  Array[String]()
          // right border
          else if (maxScoreLocalIndexFirst + Nngrams == nTerms)  terms.slice(maxScoreLocalIndexFirst-1, maxScoreLocalIndexFirst+Nngrams)
          else  Array[String]()
      }
      // get ngram on right side
      else if (direction == "right") {
          // if ngram is on left border of term arrayri
          if ( (maxScoreLocalIndexFirst > 0) & (maxScoreLocalIndexFirst+Nngrams < nTerms) )  terms.slice(maxScoreLocalIndexFirst+1, maxScoreLocalIndexFirst+Nngrams+2)
          // left border
          else if (maxScoreLocalIndexFirst == 0)  terms.slice(0, Nngrams+2)
          // right border
          else if (maxScoreLocalIndexFirst + Nngrams == nTerms)  Array[String]()
          else  Array[String]()

      } else {
          println("ERROR: Wrong direction provided in func getNgram(). Possible choices: [left, right]")
          Array[String]()
      }
  }

    def search(query:String, maxHits:Int, filter:Row = Row.empty, outFields:Seq[StructField]=Seq[StructField](), maxLevDistance:Int=2 , minScore:Double=0.0,
              boostAcronyms:Boolean=false, Nngrams:Int= -1 ) = {


//>>>>>>> ngram-search
        val terms = (if(query == null) "" else  query).replaceAll("[^\\p{L}]+", ",").split(",").filter(s => s.length>0)
        val nTerms = terms.length

        var maxScoreLocal: (Array[String], Float, Document, String, StructType ) = (Array[String](), 0.0f, null, "", null)
        var maxScoreLocalIndexFirst:Int = 0
        var ngramLocalLeft = Array[String]()
        var ngramLocalRight = Array[String]()
        var temp: (Array[String], Float, Document, String, StructType ) = (Array[String](), 0.0f, null, "", null)
        var temp2: (Array[String], Float, Document, String, StructType ) = (Array[String](), 0.0f, null, "", null)

        if(query!=null) {

          if ( (terms.length > Nngrams) & (Nngrams != -1) ) {
            maxScoreLocal = terms.sliding(Nngrams).map( ngram => score_terms(ngram, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits) )
                                  .reduce( (x,y) => (x, y) match {case ((ngram1, score1, doc1, info1, outSchema1),(ngram2, score2, doc2, info2, outSchema2)) =>
                                                                if(score1 > score2) (ngram1, score1, doc1, info1, outSchema1) else (ngram2, score2, doc2, info2, outSchema2)}     // get ngram with maximum score
                                  )

            maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                       // else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                    } // get index of first term in ngram

            var breakLoop = false
            ngramLocalLeft = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="left")
            ngramLocalRight = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="right")


            while ( !(ngramLocalLeft.isEmpty & ngramLocalRight.isEmpty) & (breakLoop==false) ) {
                println("maxScoreLocal: "+maxScoreLocal._2+" ngram: "+maxScoreLocal._1.mkString(", ")+" maxIndexFirst: "+maxScoreLocalIndexFirst+"\n")
                println("ngram Left: "+ngramLocalLeft.mkString(","))
                println("ngram right: "+ngramLocalRight.mkString(","))

                // calculate score for ngrams in left direction (current ngram is on right border of query)
                if (ngramLocalLeft.isEmpty & !ngramLocalRight.isEmpty) {
                    println("Only in right direction..")
                    temp = score_terms(ngramLocalRight, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits)

                    println("\t score right ngram "+temp._1.mkString(",")+" : "+temp._2)

                    // if in this direction found score is higher, continue to check for larger ngrams
                    if (temp._2 > maxScoreLocal._2) {
                        maxScoreLocal = temp
                        maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                            //else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                            } // get index of first term in ngram

                        ngramLocalRight = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="right")
                        println("\t found higher score in right direction: "+maxScoreLocal._2)

                    }
                    // if already found score is the highest, break while
                    else {
                        println("\t current ngram has highest score: "+maxScoreLocal._2)
                        breakLoop = true
                    }
                }
                // calculate score for ngrams in right direction (current ngram is on left border of query)
                else if (!ngramLocalLeft.isEmpty & ngramLocalRight.isEmpty) {
                    println("only in left direction..")

                    temp = score_terms(ngramLocalLeft, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits)

                    println("\t score left ngram "+temp._1.mkString(",")+" : "+temp._2)

                    // if in this direction found score is higher, continue to check for larger ngrams
                    if (temp._2 > maxScoreLocal._2) {
                        maxScoreLocal = temp
                        maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                            //else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                            } // get index of first term in ngram

                        ngramLocalLeft = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="left")
                        println("\t found higher score in left direction: "+maxScoreLocal._2)

                    }
                    // if already found score is the highest, break while
                    else {
                        breakLoop = true
                        println("\t current ngram has highest score: "+maxScoreLocal._2)
                    }

                }
                // calculate score for ngrams in both directions
                else if (!ngramLocalLeft.isEmpty & !ngramLocalRight.isEmpty) {
                    println("in both directions..")


                    temp = score_terms(ngramLocalLeft, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits)
                    temp2 = score_terms(ngramLocalRight, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits)
                    println("\t ngramleft:"+temp._1.mkString(",")+" score: "+temp._2)
                    println("\t ngramright:"+temp2._1.mkString(",")+" score: "+temp2._2)

                    // ngram to left side is higher than current ngram
                    if (temp._2 > maxScoreLocal._2) {

                        // ngram of left side is also higher than right ngram
                        if (temp._2 > temp2._2) {
                            maxScoreLocal = temp
                            maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                                //else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                                } // get index of first term in ngram
                            ngramLocalLeft = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="left")
                            ngramLocalRight = Array[String]()
                            println("\t left ngram highest score: "+maxScoreLocal._2)

                        }
                        // right ngram is higher than left one
                        else {
                            maxScoreLocal = temp2
                            maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                                //else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                                } // get index of first term in ngram
                            ngramLocalLeft = Array[String]()
                            ngramLocalRight = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="left")
                            println("\t right ngram highest score"+maxScoreLocal._2)
                        }
                    // current ngram is higher than left one
                    } else {
                        // right ngram is higher than current one
                        if (temp2._2 > maxScoreLocal._2) {
                            maxScoreLocal = temp2
                            maxScoreLocalIndexFirst = terms.sliding(Nngrams).indexWhere {
                                        case Array(term1) => (term1 == maxScoreLocal._1(0))
                                        case Array(term1, term2) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1))
                                        case Array(term1, term2, term3) => (term1 == maxScoreLocal._1(0)) & (term2 == maxScoreLocal._1(1)) & (term3 == maxScoreLocal._1(2))
                                                //else println("ERROR: Nngrams only implemented for n=1, 2, 3!")
                                                } // get index of first term in ngram
                            //}
                            ngramLocalLeft = Array[String]()
                            ngramLocalRight = getNgram(terms, nTerms, Nngrams, maxScoreLocalIndexFirst, direction="left")
                            println("\t right ngram highest score"+maxScoreLocal._2)

                        }
                        // current ngram is highest -> return
                        else {
                            println("\t current ngram highest score"+maxScoreLocal._2)
                            breakLoop = true
                        }
                    }
                }
                // should not occur
                else println("WEEEEEIRD")
            }
          } else {
            maxScoreLocal = score_terms(terms, query, maxLevDistance, filter=filter, outFields=outFields, maxHits=maxHits)
          }

          val (ngram, score, doc, info, outSchema) = maxScoreLocal

//          if(score < minScore) //None
          if(score < minScore) Array[GenericRowWithSchema]()
          else {
            Array(new GenericRowWithSchema(
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
                }}) ++ Array(score)
              ,schema = outSchema))
          }

        }
        else Array[GenericRowWithSchema]()


      //   if(query!=null) {
      //     hits.flatMap(hit => {
      //       if(hit.score < minScore) None
      //       else {
      //         val doc = searcher.doc(hit.doc)
      //         Some(new GenericRowWithSchema(
      //           values = outFields.toArray.map(field => {
      //             val lucField = doc.getField(field.name)
      //             if(field.name == null || lucField == null) null
      //             else
      //               field.dataType match {
      //             case dt:StringType => lucField.stringValue
      //             case dt:IntegerType => lucField.numericValue().intValue()
      //             case dt:BooleanType => lucField.binaryValue().bytes(0) == 1.toByte
      //             case dt:LongType =>  lucField.numericValue().longValue()
      //             case dt:FloatType => lucField.numericValue().floatValue()
      //             case dt:DoubleType => lucField.numericValue().doubleValue()
      //             case dt => {
      //               var obj:Any = null
      //               val serData= lucField.binaryValue().bytes;
      //               if (serData!=null) {
      //                  val in=new ObjectInputStream(new ByteArrayInputStream(serData))
      //                  obj = in.readObject()
      //                  in.close()
      //               }
      //               obj
      //             }
      //             }}) ++ Array(hit.score)
      //           ,schema = outSchema))
      //       }
      //   })
      // } else Array[GenericRowWithSchema]()
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
//    def close(deleteLocal:Boolean = false) {
//        val dir = tmpIndex.getDirectory().toString
//        if(new java.io.File(dir).exists()) {
//            if(deleteLocal)
//                this.deleteRecurse(dir)
//        }
//        tmpIndex.close
//        reader.close
//>>>>>>> ngram-search
//    }
}

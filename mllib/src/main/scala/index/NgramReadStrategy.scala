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



case class Ngram(text:Array[String], startIndex:Int, endIndex:Int)
case class SearchMatch(docId:Int, score:Float, ngram:Ngram)

case class NgramReadStrategy(searcher:IndexSearcher, indexDirectory:LocalNode, reader:DirectoryReader,
                             usePopularity:Boolean = false, Nngrams:Int = -1 ) extends IndexReaderStrategy {


  // calculate score for a given term/ngram

//  def score_terms(Ngram:Ngram, maxHits:Int, strategy:IndexReaderStrategy, maxLevDistance:Int=2, filter:Row = Row.empty): Option[SearchMatch] = {
  def score_terms(Ngram:Ngram, maxHits:Int, maxLevDistance:Int=2, filter:Row = Row.empty): Option[SearchMatch] = {
    val terms = Ngram.text
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


    val docs = this.searcher.search(q, maxHits);
    val hits = docs.scoreDocs;

    if (hits.size > 0) {
        val best = hits.reduce( (hit1, hit2) => { if (hit1.score > hit2.score) hit1 else hit2} )
        Some(SearchMatch(docId=best.doc, score=best.score, ngram=Ngram))
    }
    else
        None

    }

    // get expanded Ngram in left or right direction of the provided term
    def getNgram(currentNgram:Ngram, terms:Array[String], Nngrams:Int, direction:String="left"): Option[Ngram] = {

        val nTerms = terms.length

        // get ngram on left side
        if (direction == "left") {
            // if ngram is neither on the left nor the right border
            if ( (currentNgram.startIndex > 0) & (currentNgram.endIndex < nTerms) )  Some(Ngram(text=terms.slice(currentNgram.startIndex-1, currentNgram.endIndex), startIndex=currentNgram.startIndex-1, endIndex=currentNgram.endIndex))
            // left border
            else if (currentNgram.startIndex == 0)  None
            // right border
            else if (currentNgram.endIndex == nTerms)  Some(Ngram(text=terms.slice(currentNgram.startIndex-1, currentNgram.endIndex), startIndex=currentNgram.startIndex-1, endIndex=currentNgram.endIndex))
            else None
        }
        // get ngram on right side
        else if (direction == "right") {
            // if ngram is neither on the left nor the right border
            if ( (currentNgram.startIndex > 0) & (currentNgram.endIndex < nTerms) )  Some(Ngram(text=terms.slice(currentNgram.startIndex, currentNgram.endIndex+1), startIndex=currentNgram.startIndex, endIndex=currentNgram.endIndex+1))
            // left border
            else if (currentNgram.startIndex == 0)  Some(Ngram(text=terms.slice(0, Nngrams+1), startIndex=0, endIndex=Nngrams+1))
            // right border
            else if (currentNgram.endIndex == nTerms)  None
            else  None

        } else {
            println("ERROR: Wrong direction provided in func getNgram(). Possible choices: [left, right]")
            None
        }
    }

    // search in expanded Ngrams until max score is found
    def search_Ngram_expanded(maxScoreLocal:SearchMatch, terms:Array[String], Nngrams:Int, nTerms:Int,
                              maxHits:Int, maxLevDistance:Int=2,
                              filter:Row = Row.empty): Option[SearchMatch] = {

      var maxScoreLocalNgram:Option[SearchMatch] = Some(maxScoreLocal)
      var ngramLocalLeft:Option[Ngram] = null
      var ngramLocalRight:Option[Ngram] = null
      var temp:Option[SearchMatch] = null
      var temp2:Option[SearchMatch] = null

      var breakLoop = false

      ngramLocalLeft = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="left")
      ngramLocalRight = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="right")

      while ( !(ngramLocalLeft.isEmpty && ngramLocalRight.isEmpty) && (breakLoop==false) ) {

          // calculate score for ngrams in right direction (current ngram is on left border of query)
          if (ngramLocalLeft.isEmpty && !ngramLocalRight.isEmpty) {
              temp = score_terms(ngramLocalRight.get, maxHits=maxHits,
                                 maxLevDistance=maxLevDistance, filter=filter)

              // if in this direction found score is higher, continue to check for larger ngrams
              if (temp.get.score > maxScoreLocalNgram.get.score) {
                  maxScoreLocalNgram = temp
                  ngramLocalRight = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="right")
              }
              // if already found score is the highest, break while
              else breakLoop = true
          }
          // calculate score for ngrams in left direction (current ngram is on right border of query)
          else if (!ngramLocalLeft.isEmpty && ngramLocalRight.isEmpty) {
              temp = score_terms(ngramLocalLeft.get, maxHits=maxHits,
                                 maxLevDistance=maxLevDistance, filter=filter)

              // if in this direction found score is higher, continue to check for larger ngrams
              if (temp.get.score > maxScoreLocalNgram.get.score) {
                  maxScoreLocalNgram = temp
                  ngramLocalLeft = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="left")
              }
              // if already found score is the highest, break while
              else breakLoop = true
          }
          // calculate score for ngrams in both directions
          else if (!ngramLocalLeft.isEmpty && !ngramLocalRight.isEmpty) {
              temp = score_terms(ngramLocalLeft.get, maxHits=maxHits,
                                 maxLevDistance=maxLevDistance, filter=filter)
              temp2 = score_terms(ngramLocalRight.get, maxHits=maxHits,
                                  maxLevDistance=maxLevDistance, filter=filter)

              // ngram to left side is higher than current ngram
              if (temp.get.score > maxScoreLocalNgram.get.score) {

                  // ngram of left side is also higher than right ngram
                  if (temp.get.score > temp2.get.score) {
                      maxScoreLocalNgram = temp
                      ngramLocalLeft = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="left")
                      ngramLocalRight = None
                  }
                  // right ngram is higher than left one
                  else {
                      maxScoreLocalNgram = temp2
                      ngramLocalLeft = None
                      ngramLocalRight = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="right")
                  }
              // current ngram is higher than left one
              } else {
                  // right ngram is higher than current one
                  if (temp2.get.score > maxScoreLocalNgram.get.score) {
                      maxScoreLocalNgram = temp2
                      ngramLocalLeft = None
                      ngramLocalRight = getNgram(maxScoreLocalNgram.get.ngram, terms, Nngrams, direction="right")
                  }
                  // current ngram is highest -> return
                  else breakLoop = true
              }
          }
          // should not occur
          else throw new Exception("THIS SHOULD NOT OCCUR! CHECK NGRAM SEARCH!")
      }

      maxScoreLocalNgram
    }


    def searchDoc(query:String, maxHits:Int, filter:Row = Row.empty, maxLevDistance:Int=2 , minScore:Double=0.0,
              boostAcronyms:Boolean=false) = {


        val terms = (if(query == null) "" else  query).replaceAll("[^\\p{L}]+", ",").split(",").filter(s => s.length>0)
        val nTerms = terms.length

        var maxScoreLocal:Seq[SearchMatch] = null

        if ( (terms.length > Nngrams) && (Nngrams != -1) ) {

            // calculate score for each ngram and take the N with the highest score
            maxScoreLocal = terms.zipWithIndex.sliding(Nngrams)
                                                  .map{arrayOfStrings => Ngram(arrayOfStrings.map{case (term, index) => term}, arrayOfStrings(0)._2, arrayOfStrings(0)._2+Nngrams) }
                                                  .flatMap(ngram => score_terms(ngram, maxHits=maxHits, maxLevDistance=maxLevDistance, filter=filter))
                                                  .toSeq
                                                  .topN(maxHits, (searchMatch1, searchMatch2) => (searchMatch1.score < searchMatch2.score)  )


            // For each Ngram with the highest score, check if score can be improved by expanded Ngram
            maxScoreLocal = maxScoreLocal.flatMap( maxHit =>  search_Ngram_expanded(maxHit, terms, Nngrams, nTerms, maxHits) )
                                         .toSeq
                                         .topN(maxHits, (searchMatch1, searchMatch2) => (searchMatch1.score < searchMatch2.score) )

        } else {
          val temp = score_terms(Ngram(terms, -1, -1), maxHits=maxHits, maxLevDistance=maxLevDistance, filter=filter)
          if (!temp.isEmpty) maxScoreLocal = Seq(temp.get)
          else Array[SearchMatch]()
          //maxScoreLocal = Seq( score_terms(Ngram(terms, -1, -1), maxHits=maxHits, maxLevDistance=maxLevDistance, filter=filter).get )
        }
        maxScoreLocal.toArray
    }

}

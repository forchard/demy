package demy.mllib.test.index

import demy.mllib.test.UnitTest
import demy.mllib.test.SharedSpark
import demy.mllib.index.implicits._
import org.apache.spark.sql.functions.{col}
import demy.storage.Storage

trait ImplicitsSpec extends UnitTest {

  lazy val leftDF  =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq("foo", "bar").toDS.toDF("query")
  }
  lazy val rightDF =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq(("this is bar", 99)
      , ("I am out of here!", 99))
        .toDS.toDF("text", "val")
  }

  lazy val lookedUp = leftDF.luceneLookup(right = rightDF
                                 , query = col("query")
                                 , text=col("text")
				                         , maxLevDistance=0
                                 , indexPath=Storage.getLocalStorage.getTmpPath()
                                 , reuseExistingIndex=false
                                 , leftSelect=Array(col("*"))
                                 , rightSelect=Array(col("*"))
                                 , popularity=None
                                 , indexPartitions = 1
                                 , maxRowsInMemory=10
                                 , indexScanParallelism= 1
                                 , tokenizeText = true)

  lazy val findPerfectMatch = {
    val spark = this.getSpark
    import spark.implicits._
    lookedUp.where(col("text").isNotNull).select($"query", $"text", $"val").as[(String, String, Int)]
  }

  "Lucene Lookup" should "find perfect match in text and get a value" in {
    assert(findPerfectMatch.collect.toSeq == Seq(("bar", "this is bar", 99)))
  }



  lazy val leftDFMinScore  =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq("Twitter", "Fort Worth").toDS.toDF("query")
  }
  lazy val rightDFMinScore =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq(("Titter Khel")
      , ("I am out of here!")
      , ("Fort Worth, TX"))
        .toDS.toDF("text")
  }

  lazy val lookedUpMinScore = leftDFMinScore.luceneLookup(right = rightDFMinScore
                                 , query = col("query")
                                 , text=col("text")
				 , maxLevDistance=0
                                 , indexPath=Storage.getLocalStorage.getTmpPath()
                                 , reuseExistingIndex=false
                                 , leftSelect=Array(col("*"))
                                 , rightSelect=Array(col("*"))
                                 , popularity=None
                                 , indexPartitions = 1
                                 , maxRowsInMemory=10
                                 , indexScanParallelism= 1
                                 , tokenizeText = true
                                 , minScore = 1.0
                                 )



  lazy val findPerfectMatchMinScore = {
    val spark = this.getSpark
    import spark.implicits._
    lookedUpMinScore.where(col("text").isNotNull).select($"query", $"text").as[(String, String)]
  }

  it should "exclude Spam like 'Twitter' with a low score (minScore=1.0)" in {
    assert(findPerfectMatchMinScore.collect.toSeq == Seq(("Fort Worth", "Fort Worth, TX")))
  }





  lazy val leftDFAcronyms  =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq("Fort Worth, TX", "Des Plaines, IL", "IL", "OH", "Columbus, OH").toDS.toDF("query")
  }
  lazy val rightDFAcronyms=  {
    val spark = this.getSpark
    import spark.implicits._
    Seq(("Illinois, IL")
      , ("Wœrth, FR")
      , ("Fort Worth, TX")
      , ("Des Plaines, US, IL")
      , ("Sainte-Anne-des-Plaines, CA")
      , ("Beau Bassin MU")
      , ("Ohio, OH, US")
      , ("Columbus, US, OH")
      , ("Columbus, NI")
      )
        .toDS.toDF("text")
  }

  lazy val lookedUpAcronyms = leftDFAcronyms.luceneLookup(right = rightDFAcronyms
                                 , query = col("query")
                                 , text=col("text")
				                         , maxLevDistance=0
                                 , indexPath=Storage.getLocalStorage.getTmpPath()
                                 , reuseExistingIndex=false
                                 , leftSelect=Array(col("*"))
                                 , rightSelect=Array(col("*"))
                                 , popularity=None
                                 , indexPartitions = 1
                                 , maxRowsInMemory=10
                                 , indexScanParallelism= 1
                                 , tokenizeText = true
                                 , boostAcronyms=true
                                 )



  lazy val findPerfectMatchAcronyms = {
    val spark = this.getSpark
    import spark.implicits._
    lookedUpAcronyms.where(col("text").isNotNull).select($"query", $"text").as[(String, String)]
  }

  it should "find 2 letter acronyms in text" in {
    assert(findPerfectMatchAcronyms.collect.toSeq == Seq(("Fort Worth, TX", "Fort Worth, TX"),
                                                         ("Des Plaines, IL", "Des Plaines, US, IL"),
                                                         ("IL", "Illinois, IL"),
                                                         ("OH", "Ohio, OH, US"),
                                                         ("Columbus, OH", "Columbus, US, OH")
                                                         ))
  }



  lazy val leftNgrams  =  {
    val spark = this.getSpark
    import spark.implicits._
    Seq("In South east of CA lies the beauty of Glasgow in Scotland England").toDS.toDF("query")
  }
  lazy val rightNgrams=  {
    val spark = this.getSpark
    import spark.implicits._
    Seq(("Wœrth, FR")
      , ("Sainte-Anne-des-Plaines, CA")
      , ("Cardiff")
      , ("South Wales Valley")
      , ("New Glasgow, Californien CA")
      , ("North west Glasgow GB Scotland")
      , ("Glasgow South France")
      )
        .toDS.toDF("text")
  }

  lazy val lookedUpNgrams = leftNgrams.luceneLookup(right = rightNgrams
                                 , query = col("query")
                                 , text=col("text")
				                         , maxLevDistance=0
                                 , indexPath=Storage.getLocalStorage.getTmpPath()
                                 , reuseExistingIndex=false
                                 , leftSelect=Array(col("*"))
                                 , rightSelect=Array(col("*"))
                                 , popularity=None
                                 , indexPartitions = 1
                                 , maxRowsInMemory=10
                                 , indexScanParallelism= 1
                                 , tokenizeText = true
                                 , strategy="demy.mllib.index.NgramStrategy"
                                 , strategyParams=Map( ("nNgrams", "3"))
                                 )


  lazy val findPerfectMatchNgrams = {
    val spark = this.getSpark
    import spark.implicits._
    lookedUpNgrams.where(col("text").isNotNull).select($"query", $"text").as[(String, String)]
  }

  it should "find the right place using ngrams" in {
    assert(findPerfectMatchNgrams.collect.toSeq == Seq(("In South east of CA lies the beauty of Glasgow in Scotland England",
                                                        "North west Glasgow GB Scotland")
                                                         ))
  }



}

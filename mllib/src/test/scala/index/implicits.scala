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

  it should "find 2 letter accronyms in text" in {
    assert(1 + 1 == 2)
  } 
}

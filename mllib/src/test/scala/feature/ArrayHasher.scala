package demy.mllib.test.feature

import demy.mllib.test.UnitTest
import demy.mllib.test.SharedSpark
import demy.mllib.feature.ArrayHasher
import org.apache.spark.ml.linalg.SparseVector


trait ArrayHasherSpec extends UnitTest{
  val AHRangeSize = 1000
  val fixedTokens = Seq("This","is","the", "best", "on" , "the", "earth", "!")
  val numSame = fixedTokens.distinct.size
  val numTwo = 1
  val numSingle = 6
  def fixedDF =  {
    val spark = this.getSpark
    import spark.implicits._
    Range(0, AHRangeSize).toSeq.map(i => fixedTokens).toDS.toDF("tokens")
  }
  val numF = Math.pow(2, 11).toInt 

  val hasher = new ArrayHasher().setInputCol("tokens").setOutputCol("vector").setNumFeatures(numF)

  "Array Hasher " should "produce Sparse Vectors" in {
    assert(hasher.transform(fixedDF).head.getAs[Any]("vector").isInstanceOf[SparseVector]) 
  }
  it should "keep vector size as the provided size" in {
    assert(hasher.transform(fixedDF).head.getAs[SparseVector]("vector").size == numF )
  }
  it should "keep indices size as number of unique values" in {
    assert(hasher.transform(fixedDF).head.getAs[SparseVector]("vector").indices.size == numSame )
  }
  it should "produce a vector wich components adds up to tne number of tokens" in {
    assert(hasher.transform(fixedDF).head.getAs[SparseVector]("vector").values.sum == fixedTokens.size.toDouble) 
  }
  it should "produce 1.0 values for elements being a single time" in {
    assert(hasher.transform(fixedDF).head.getAs[SparseVector]("vector").values.filter(_ == 1.0).size == numSingle )
  }
  it should "produce 2.0 values for elements being twice" in {
    assert(hasher.transform(fixedDF).head.getAs[SparseVector]("vector").values.filter(_ == 2.0).size == numTwo )
  }
  it should "always return same position for same words" in {
    assert(hasher.transform(fixedDF).distinct.count == 1 )
  }
}

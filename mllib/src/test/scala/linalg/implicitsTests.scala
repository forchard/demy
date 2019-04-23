package demy.mllib.test.linalg

import org.scalatest._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector => MLVector, Vectors}
import demy.mllib.test.{UnitTest, UnitTestVars}
import demy.mllib.linalg.implicits._

trait implicitsSpec extends UnitTest{
  import demy.mllib.test.linalg.{implicitsSpecVars => v}  
 
  "Sum" should "be correct for dense vectors and sparse vectors" in {
    assert(v.vectors.map{case(a, b) => a.sum(b)}.zip(v.vectors.map{case(a, b)=>v.sum(a, b)}).map{case(a, b) => v.error(a, b)}.sum < 0.00001) 
  }
  "Minus" should "be correct for dense vectors" in {
    assert(v.vectors.map{case(a, b) => a.minus(b)}.zip(v.vectors.map{case(a, b)=>v.minus(a, b)}).map{case(a, b) => v.error(a, b)}.sum < 0.00001) 
  }

  "Cosine Similarity" should "be 1 for equal vectors" in {
    assert(v.vectors.map{case(a, b) =>a.cosineSimilarity(a)}.map(s => Math.abs(1.0 - s)).sum < 0.00001 )
  }

  it should  "be equal to calculated test cosine similarity" in {
    
    assert(v.vectors.map{case(a, b) =>(a.cosineSimilarity(b), v.cosineSimilarity(a, b))}.map{case(a, b)=> Math.abs(a - b)}.sum < 0.00001) 
  }
}

object implicitsSpecVars extends UnitTestVars {
  val densePairs = Seq((Vectors.dense(1.0, -2.0, 4.0, 10.0), Vectors.dense(1.0, 2.0, 7.0, -10.0))
                        ,(Vectors.dense(1.0, 2.0, 7.0, 10.0), Vectors.dense(1.0, -2.0, 34.0, 9.9))
                        ,(Vectors.dense(1.0, 2.0, 7.0, 10.0), Vectors.dense(-1.0, -2.0, -34.0, -9.9))
                     )
  val sparsePairs = Seq((new SparseVector(100, Array(0, 6, 99), Array(2.0, 1.0, 0.5)), new SparseVector(100, Array(0, 6, 99), Array(2.0, 1.0, 0.5)))
                            ,(new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)), new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)))
                            ,(new SparseVector(100, Array(3, 8), Array(2.0, 1.0)), new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)))
                )  

  val vectors = densePairs ++ sparsePairs

  def cosineSimilarity(x: MLVector, y: MLVector): Double = {
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  def sum(x: MLVector, y: MLVector): MLVector = {
    Vectors.dense(x.toArray.zip(y.toArray).map{case(a, b) => a + b})
  }
  def minus(x: MLVector, y: MLVector): MLVector = {
    Vectors.dense(x.toArray.zip(y.toArray).map{case(a, b) => a - b})
  }
  def error(x: MLVector, y: MLVector): Double = {
    x.toArray.zip(y.toArray).map{case(a, b) => Math.abs(a - b)}.sum
  }

  def dotProduct(x: MLVector, y: MLVector): Double = {
    (for((a, b) <- x.toArray zip y.toArray) yield a * b) sum
  }
  def magnitude(x: MLVector): Double = {
    math.sqrt(x.toArray map(i => i*i) sum)
  }
}

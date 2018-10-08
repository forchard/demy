package demy.mllib.test.linalg

import org.scalatest._
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import demy.mllib.test.UnitTest
import demy.mllib.linalg.implicits._

trait implicitsSpec extends UnitTest{

  val toTestValues = Seq((Seq(1.0, -2.0, 4.0, 10.0), Seq(1.0, 2.0, 7.0, -10.0))
                        ,(Seq(1.0, 2.0, 7.0, 10.0), Seq(1.0, -2.0, 34.0, 9.9))
                        ,(Seq(1.0, 2.0, 7.0, 10.0), Seq(-1.0, -2.0, -34.0, -9.9))
                     )
  val toTestSparseness = Seq((new SparseVector(100, Array(0, 6, 99), Array(2.0, 1.0, 0.5)), new SparseVector(100, Array(0, 6, 99), Array(2.0, 1.0, 0.5)))
                            ,(new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)), new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)))
                            ,(new SparseVector(100, Array(3, 8), Array(2.0, 1.0)), new SparseVector(100, Array(3, 6, 45), Array(2.0, 1.0, 0.5)))
                )  
  val t = new implicitsSpecTool

  "Cosine Similarity" should "be 1 for equal vectors" in {
    assert(toTestValues.map(p => t.cosineSimAllError(p._1, p._1)).sum<0.000001) 
  }

  it should  "be equal to calculated test cosine similarity" in {
    assert(toTestValues.map(p => t.cosineSimAllError(p._1, p._2)).sum<0.000001) 
  }

  it should  "be equal to calculated test cosine similarity for sparse vectors" in {
    assert(toTestSparseness.map(p => t.cosineSimAllError(p._1, p._2)).sum<0.000001) 
  }
}

class implicitsSpecTool {
  def cosineSimAllError(v1:Any, v2:Any) : Double = {

    val results =  (v1, v2) match {
     case (d1:DenseVector, d2:DenseVector) => 
        Seq(cosineSimilarity(d1.values, d2.values), IterableUtil(d1.values).cosineSimilarity(d2.values), d1.cosineSimilarity(d2), d1.toSparse.cosineSimilarity(d2.toSparse))
     case (d1:SparseVector, d2:SparseVector)=>  
        Seq(cosineSimilarity(d1.toDense.values, d2.toDense.values), IterableUtil(d1.toDense.values).cosineSimilarity(d2.toDense.values), d1.toDense.cosineSimilarity(d2.toDense), d1.cosineSimilarity(d2))
     case (vv1:Iterable[_], vv2:Iterable[_]) => { 
        val (v1, v2) = (vv1.map(_.asInstanceOf[Double]), vv2.map(_.asInstanceOf[Double]))
        Seq(cosineSimilarity(v1, v2), v1.cosineSimilarity(v2), Vectors.dense(v1.toArray).cosineSimilarity(Vectors.dense(v2.toArray)), Vectors.dense(v1.toArray).toSparse.cosineSimilarity(Vectors.dense(v2.toArray).toSparse))
        }
     case (o1, o2)  => throw new Exception(s"Cannot execute vector test on (${o1.getClass.getName}, ${o2.getClass.getName}) @epi") 
   }

   results.fold(0.0)((currentError, next) => currentError + Math.abs(next - results(0)))
  }

  def cosineSimilarity(x: Iterable[Double], y: Iterable[Double]): Double = {
    dotProduct(x, y)/(magnitude(x) * magnitude(y))
  }
  def dotProduct(x: Iterable[Double], y: Iterable[Double]): Double = {
    (for((a, b) <- x zip y) yield a * b) sum
  }
  def magnitude(x: Iterable[Double]): Double = {
    math.sqrt(x map(i => i*i) sum)
  }
}


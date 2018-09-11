package demy.mllib.linalg;

import demy.mllib.util.MergedIterator

object implicits {
  implicit class VectorUtil(val left: Iterable[Double]) {
    def cosineSimilarity(right:Iterable[Double]) = {
      val (dotProduct, normLeft, normRight) = 
        MergedIterator(left.iterator, right.iterator, 0.0, 0.0).toIterable.foldLeft((0.0, 0.0, 0.0))((sums, current) => (sums, current) match {case ((dotProduct, normLeft, normRight), (lVal, rVal)) =>
          (dotProduct + lVal * rVal
            ,normLeft +  Math.pow(lVal, 2)
            ,normRight + Math.pow(rVal, 2))
        })
        dotProduct / (Math.sqrt(normLeft) * Math.sqrt(normRight));
      } 
  }
}

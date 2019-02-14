package demy.mllib.linalg;

import demy.mllib.util.MergedIterator
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

object implicits {
  implicit class IterableUtil(val left: Iterable[Double]) {
    def cosineSimilarity(right:Iterable[Double]) = {
      val (dotProduct, normLeft, normRight) = 
        MergedIterator(left.iterator, right.iterator, 0.0, 0.0).toIterable.foldLeft((0.0, 0.0, 0.0))((sums, current) => (sums, current) match {case ((dotProduct, normLeft, normRight), (lVal, rVal)) =>
          (dotProduct + lVal * rVal
            ,normLeft +  lVal * lVal
            ,normRight + rVal * rVal)
        })
        dotProduct / (Math.sqrt(normLeft) * Math.sqrt(normRight))
      } 
  }
  implicit class VectorUtil(val left: Vector) {
    def cosineSimilarity(right:Vector) = {
      val (dotProduct, normLeft, normRight) = 
        VectorsIterator(left, right).toIterable.foldLeft((0.0, 0.0, 0.0))((sums, current) => (sums, current) match {case ((dotProduct, normLeft, normRight), (i, (lVal, rVal))) =>
          (dotProduct + lVal * rVal
            ,normLeft +  lVal * lVal
            ,normRight + rVal * rVal)
        })
        dotProduct / (Math.sqrt(normLeft) * Math.sqrt(normRight))
    }
    def similarityScore(right:Vector) = (left.cosineSimilarity(right) + 1.0)/2.0
    def sum(right:Vector) = {
      val values = VectorsIterator(left, right)
      (left, right) match {
         case (sparseLeft:SparseVector, sparseRight:SparseVector) => {
           val nonEmpty = values.filter(p => p match {case (index, (valLeft, valRight)) => valLeft!=0.0 || valRight!=0.0}).toArray
           Vectors.sparse(
             size = nonEmpty.last._1+1
             , indices = nonEmpty.map(p => p match {case (index, (lVal, rVal)) => index})
             , values = nonEmpty.map(p => p match {case (index, (lVal, rVal)) => lVal + rVal})
           )}
         case (denseLeft:DenseVector, denseRight:DenseVector) => Vectors.dense(values.map(p => p match {case (index, (lVal, rVal)) => lVal + rVal}).toArray)
         case _ => throw new Exception("Cannot build an iterator on differentr vector types @epi")
       }
      } 
    def minus(right:Vector) = {
      val values = VectorsIterator(left, right)
      (left, right) match {
         case (sparseLeft:SparseVector, sparseRight:SparseVector) => {
           val nonEmpty = values.filter(p => p match {case (index, (valLeft, valRight)) => valLeft!=0.0 || valRight!=0.0}).toArray
           Vectors.sparse(
             size = nonEmpty.last._1+1
             , indices = nonEmpty.map(p => p match {case (index, (lVal, rVal)) => index})
             , values = nonEmpty.map(p => p match {case (index, (lVal, rVal)) => lVal - rVal})
           )}
         case (denseLeft:DenseVector, denseRight:DenseVector) => Vectors.dense(values.map(p => p match {case (index, (lVal, rVal)) => lVal + rVal}).toArray)
         case _ => throw new Exception("Cannot build an iterator on differentr vector types @epi")
       }
      } 
    def scale(factor:Double) = {
      left match { 
        case vec:SparseVector => Vectors.sparse(size = vec.size, indices = vec.indices, values = vec.values.map(v => v * factor))
        case vec:DenseVector => Vectors.dense(values = vec.values.map(v => v * factor))
        case _ => throw new Exception("Cannot build an iterator on differentr vector types @epi")
      }
    }
  }
}
case class SparseVectorsIterator(left:SparseVector, right:SparseVector) extends Iterator[(Int, (Double, Double))] {
      val leftIterator = MergedIterator(left.indices.iterator, left.values.iterator, Int.MaxValue, 0.0) 
      val rightIterator = MergedIterator(right.indices.iterator, right.values.iterator, Int.MaxValue, 0.0)
      
      var leftPair = (-1, 0.0)
      var rightPair = (-1, 0.0)

      def hasNext = ((leftIterator.hasNext || rightIterator.hasNext) && leftPair._1 == -1 && rightPair._1 == -1) || (leftPair._1<Int.MaxValue && rightPair._1<Int.MaxValue)
      def next = {
        val outOfRange = (Int.MaxValue, 0.0)
        val newLeft = (if(leftPair._1 <= rightPair._1) {
                        if(leftIterator.hasNext)
                          leftIterator.next
                        else outOfRange
                       } else { leftPair }

                      )
        val newRight = (if(rightPair._1 <= leftPair._1) {
                         if(rightIterator.hasNext)
                           rightIterator.next
                         else outOfRange
                        } else { rightPair }
                       )
        leftPair = newLeft
        rightPair = newRight
        if(leftPair._1 == Int.MaxValue && rightPair._2 == Int.MaxValue) throw new Exception("There are no further values on vectors @epi")

        if(leftPair._1 < rightPair._1) (leftPair._1, (leftPair._2, 0.0))
        else if (leftPair._1 > rightPair._1) (rightPair._1, (0.0, rightPair._2))
        else (leftPair._1, (leftPair._2, rightPair._2))
      }
}

case class DenseVectorsIterator(left:DenseVector, right:DenseVector) extends Iterator[(Int, (Double, Double))] {
       val jointIterator = MergedIterator(left.values.iterator, right.values.iterator, 0.0, 0.0).zipWithIndex.map(p => (p._2, p._1))
       def hasNext = jointIterator.hasNext
       def next = jointIterator.next
}

case class VectorsIterator(left:Vector, right:Vector) extends Iterator[(Int, (Double, Double))] {
       val jointIterator = (left, right) match {
         case (sparseLeft:SparseVector, sparseRight:SparseVector) => SparseVectorsIterator(sparseLeft, sparseRight)
         case (denseLeft:DenseVector, denseRight:DenseVector) => DenseVectorsIterator(denseLeft, denseRight)
         case _ => throw new Exception("Cannot build an iterator on differentr vector types @epi")
       }
       def hasNext = jointIterator.hasNext
       def next = jointIterator.next
}

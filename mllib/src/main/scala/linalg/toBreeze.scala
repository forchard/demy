package org.apache.spark.ml.linalg

object VectorPub {
  implicit class VectorPublications(val vector : Vector) extends AnyVal {
    def asBreeze : breeze.linalg.Vector[scala.Double] = vector.asBreeze
  }

  implicit class BreezeVectorPublications(val breezeVector : breeze.linalg.Vector[Double]) extends AnyVal {
    def fromBreeze : Vector = Vectors.fromBreeze(breezeVector)
  }
}

package demy.mllib.index;
import org.apache.spark.ml.linalg.{Vector => MLVector}

trait VectorIndex {
 def apply(token:Seq[String]):Map[String, MLVector]
}


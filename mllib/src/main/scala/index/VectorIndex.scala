package demy.mllib.index;
import org.apache.spark.ml.linalg.{Vector => MLVector}

trait VectorIndex {
 def apply(token:Seq[String]):Map[String, MLVector]
}

case class CachedIndex(index:VectorIndex, var cache:Map[String, MLVector]=Map[String, MLVector]()) extends VectorIndex {
  def apply(token:Seq[String]) = {
    val fromIndex = token.filter(t => !cache.contains(t))
    if(fromIndex.size > 0)
      cache ++ index(fromIndex)
    else 
      cache
  }
  def setCache(tokens:Seq[String]):this.type = {
    cache = index(tokens)
    this
  }
}

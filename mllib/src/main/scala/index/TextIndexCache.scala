package demy.mllib.index;
case class TextIndexCache(query:String, result:TextIndexResult, on:Long = System.currentTimeMillis) extends Ordered [TextIndexCache] {
  def compare (that: TextIndexCache) = this.on.compare(that.on)
}

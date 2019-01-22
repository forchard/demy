package demy.util;

object implicits {
  implicit class IterableUtil[T](val elems: Iterable[T]) {
    def topN(n:Int, smallest:(T, T) => Boolean) = {
        elems.toSeq.sortWith((a, b) => smallest(b, a)).take(n)
    }
  }

  implicit class IteratorToJava[T](it: Iterator[T]) {
    def toJavaEnumeration = EnumerationFromIterator(it)
  }
}

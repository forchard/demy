package demy.util

case class MergedIterator[T, U](a:Iterator[T], b:Iterator[U], defA:T, defB:U) extends Iterator[(T, U)] {
  def hasNext = a.hasNext || b.hasNext
  def next = (if(a.hasNext) a.next else defA, if(b.hasNext) b.next else defB)
}


case class EnumerationFromIterator[T](it:Iterator[T]) extends java.util.Enumeration[T] {
  def hasMoreElements() = it.hasNext
  def nextElement() = it.next()
}

object implicits {
  implicit class IteratorToJava[T](it: Iterator[T]) {
    def toJavaEnumeration = EnumerationFromIterator(it)
  }
}

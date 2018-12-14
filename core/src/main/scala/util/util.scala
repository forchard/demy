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

object util {
    def checkpoint[T : org.apache.spark.sql.Encoder : scala.reflect.runtime.universe.TypeTag] (ds:org.apache.spark.sql.Dataset[T], path:String)
               :org.apache.spark.sql.Dataset[T] = 
    {
        ds.write.mode("overwrite").parquet(path)
        return ds.sparkSession.read.parquet(path).as[T]
    }

    def checkpoint(df:org.apache.spark.sql.DataFrame, path:String) = 
    {
        df.write.mode("overwrite").parquet(path)
        df.sparkSession.read.parquet(path)
    }
    def getStackTraceString(e:Exception) = {
     val sw = new java.io.StringWriter();
     val pw = new java.io.PrintWriter(sw, true)
     e.printStackTrace(pw)
     sw.getBuffer().toString()
    }
/*    def schemaOf[T: scala.reflect.runtime.universe.TypeTag]: StructType = { 
        org.apache.spark.sql.catalyst.ScalaReflection
            .schemaFor[T] // this method requires a TypeTag for T
            .dataType
            .asInstanceOf[StructType] // cast it to a StructType, what spark requires as its Schema
    }*/
}

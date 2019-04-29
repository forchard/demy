package demy.util
import org.apache.spark.sql.{Dataset, DataFrame, Encoder}
import scala.reflect.runtime.universe.TypeTag
case class MergedIterator[T, U](a:Iterator[T], b:Iterator[U], defA:T, defB:U) extends Iterator[(T, U)] {
  def hasNext = a.hasNext || b.hasNext
  def next = (if(a.hasNext) a.next else defA, if(b.hasNext) b.next else defB)
}


case class EnumerationFromIterator[T](it:Iterator[T]) extends java.util.Enumeration[T] {
  def hasMoreElements() = it.hasNext
  def nextElement() = it.next()
}



object util {
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String):Dataset[T] = checkpoint(ds, path, None, false)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, reuseExisting:Boolean):Dataset[T] = checkpoint(ds, path, None, reuseExisting)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, partitionBy:Option[Array[String]]):Dataset[T] = checkpoint(ds, path, partitionBy, false)
    def checkpoint[T : Encoder : TypeTag] (ds:Dataset[T], path:String, partitionBy:Option[Array[String]], reuseExisting:Boolean):Dataset[T] =
    {  
      if((new java.io.File(path)) match {case f => !reuseExisting || !f.exists && f.listFiles().isEmpty})
        ((ds.write.mode("overwrite"), partitionBy)  match {
          case(w, Some(cols)) => w.partitionBy(cols:_*)
          case(w, _) => w
        }).parquet(path)

      ds.sparkSession.read.parquet(path).as[T]
    }

    def checkpoint(df:DataFrame, path:String, partitionBy:Option[Array[String]]=None, reuseExisting:Boolean = false):DataFrame =
    {
      if((new java.io.File(path)) match {case f => !reuseExisting || !f.exists || f.listFiles().isEmpty})
        ((df.write.mode("overwrite"), partitionBy)  match {
          case(w, Some(cols)) => w.partitionBy(cols:_*)
          case(w, _) => w
        }).parquet(path)
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

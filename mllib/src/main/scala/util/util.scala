package demy.mllib.util


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
/*    def schemaOf[T: scala.reflect.runtime.universe.TypeTag]: StructType = { 
        org.apache.spark.sql.catalyst.ScalaReflection
            .schemaFor[T] // this method requires a TypeTag for T
            .dataType
            .asInstanceOf[StructType] // cast it to a StructType, what spark requires as its Schema
    }*/
}
case class MergedIterator[T](a:Iterator[T], b:Iterator[T], defA:T, defB:T) extends Iterator[(T, T)] {
      def hasNext = a.hasNext || b.hasNext
          def next = (if(a.hasNext) a.next else defA, if(b.hasNext) b.next else defB)
}

package demy.mllib.util


case class util(dummy:Int){
};object util {
    def log(message:Any) {
        val sdfDate = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//dd/MM/yyyy
        val now = new java.util.Date();
        val strDate = sdfDate.format(now);
        println(strDate+"-->"+message);
    }
    def checkpoint[T : org.apache.spark.sql.Encoder : scala.reflect.runtime.universe.TypeTag] (ds:org.apache.spark.sql.Dataset[T], path:String):org.apache.spark.sql.Dataset[T] = {
        ds.write.mode("overwrite").parquet(path)
        return ds.sparkSession.read.parquet(path).as[T]
    }
/*    def schemaOf[T: scala.reflect.runtime.universe.TypeTag]: StructType = { 
        org.apache.spark.sql.catalyst.ScalaReflection
            .schemaFor[T] // this method requires a TypeTag for T
            .dataType
            .asInstanceOf[StructType] // cast it to a StructType, what spark requires as its Schema
    }*/
}

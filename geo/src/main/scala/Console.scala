package demy.geo
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

import java.net.URL
import java.io.{File, InputStreamReader, FileOutputStream, BufferedReader}

object Console {
  def main(args: Array[String]) {
    val spark = SparkSession
     .builder()
     .appName("demy geo console")
     .getOrCreate()
    
    val instructions = """Available commands are: 
      - shape2parquet [hfdssource] [hdfsdest]"""

    if(args==null || args.size==0 || args(0)==null) {  println(instructions); return }
    val command = args(0)
    command.toLowerCase match {
      case "shape2parquet" => {
        if(args.size<3 || args(1)==null || args(2)==null  )  { println(instructions); return }
          if(!args(1).toLowerCase.endsWith(".shp")) {
            println("Source file must be a .shp extension")
            return 
          }
          val source = args(1)
          val dest = args(2)
          val gm = new GeoManager(new Path(source), spark)
          gm.write2parquet(new Path(dest))
      }
      case _ => {
        println(instructions)
        return
      }
    } 
   }
}
  



package geodemy
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import java.io.File
import java.net.URL
import org.geotools.data.DataStoreFinder
import org.geotools.data.shapefile.dbf.DbaseFileReader
import java.nio.channels.FileChannel
import java.io.FileInputStream

import geodemy._

object Execute {
  def main(args: Array[String]) {
  val path = "/home/fod/Desktop/deleteme/CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10/CONTOURS-IRIS/1_DONNEES_LIVRAISON_2015/CONTOURS-IRIS_2-1_SHP_LAMB93_FE-2015/CONTOURS-IRIS.shp"
  val file = new File(path)

  val map = new java.util.HashMap[String, URL]()
  map.put("url", file.toURI().toURL());
  val dataStore = DataStoreFinder.getDataStore(map);
  val featureSource = dataStore.getFeatureSource(dataStore.getTypeNames()(0))
  val collection = featureSource.getFeatures();
  val iterator = collection.features();
   
  if(iterator.hasNext) {
    val f = iterator.next()
    f.getIdentifier
}
 
  val path2 = "/home/fod/Desktop/deleteme/CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10/CONTOURS-IRIS/1_DONNEES_LIVRAISON_2015/CONTOURS-IRIS_2-1_SHP_LAMB93_FE-2015/CONTOURS-IRIS.dbf"

  val in = new FileInputStream(path2).getChannel()
  val r = new DbaseFileReader(in, true, java.nio.charset.Charset.defaultCharset )
  val fields = new Array[Object](r.getHeader().getNumFields())
  r.getHeader().getFieldName(5)
  if(r.hasNext()) {
    r.readEntry(fields)
  }

  /*  val spark = SparkSession
     .builder()
     .appName("geodemy")
     .getOrCreate()
    val sql = spark.sqlContext
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration 

    val fs = FileSystem.get( hadoopConf )
    if(!fs.exists(path))
      return "";
    val in = new BufferedReader(new InputStreamReader(fs.open(path)));
  */
  }
}
  



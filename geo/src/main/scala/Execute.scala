package demy.geo
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

import java.io.File
import java.net.URL
import org.geotools.data.DataStoreFinder
import org.geotools.data.shapefile.dbf.DbaseFileReader
import java.nio.channels.FileChannel
import java.io.FileInputStream
import scala.collection.JavaConversions._

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

class GeoReader(shapefile:File) {

  def getSchema(/*shapefile:File*/):StructType = {
    val map = new java.util.HashMap[String, URL]()
    map.put("url", shapefile.toURI().toURL());
    val store = DataStoreFinder.getDataStore(map);

    val fields = store.getSchema(store.getNames.get(0).getURI).getTypes().toList.map(attr => 
      StructField(attr.getName.getURI, 
        attr.getBinding.getName match {
          case "java.lang.String" => StringType
          case "java.lang.Integer" => IntegerType
          case "java.lang.Double" => DoubleType
          case "java.lang.Float" => FloatType
          case _ => BinaryType
        }
        ,false
      )
    )
    return StructType(StructField("ID", StringType, false)::fields)   
  }   

  def getDataSet(SparkContext sc) = {
    val schema = getSchema() 
 
    val map = new java.util.HashMap[String, URL]()
    map.put("url", shapefile.toURI().toURL());
    val store = DataStoreFinder.getDataStore(map);
    val featureSource = store.getFeatureSource(store.getTypeNames()(0))   
    val collection = featureSource.getFeatures();
    val iterator = collection.features();
    val rowBuf = scala.collection.mutable.ArrayBuffer.empty[Row]    
    while(iterator.hasNext) {
      val attr = iterator.next.getAttributes()
      rowBuf += Row.fromSeq((
       Seq(row.getID) 
         ++ attr.toSeq)
       .map(e => 
            if(e.isInstanceOf[com.vividsolutions.jts.geom.Geometry]) 
              serializeToByteArray(e) 
            else 
              e
            )
       ) 
    }
    iterator.close

    val dataRDD = sc.parallelize(rowBuf)
    val encodedDF = spark.createDataFrame(dataRDD, schema)
    return encodedDF

    //val path = "/home/fod/Desktop/deleteme/CONTOURS-IRIS_2-1__SHP_LAMB93_FXX_2016-11-10/CONTOURS-IRIS/1_DONNEES_LIVRAISON_2015/CONTOURS-IRIS_2-1_SHP_LAMB93_FE-2015/CONTOURS-IRIS.shp"
/*
    
 
    if(iterator.hasNext) {
      val r = iterator.next()
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
    return StructType(Nil)
      val spark = SparkSession
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
  def serializeToByteArray(obj:Object):Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(obj)
    return b.toByteArray()
  }

}
  



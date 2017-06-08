package demy.geo
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.commons.lang.RandomStringUtils

import java.io.File
import java.net.URL
import org.geotools.data.DataStoreFinder
import org.geotools.data.DataStore
import org.geotools.data.shapefile.dbf.DbaseFileReader
import java.nio.channels.FileChannel
import scala.collection.JavaConversions._

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.FileInputStream
import java.io.FileOutputStream;

class GeoManager(shapeHDFSfile:Path, spark:SparkSession) {
  private var _dbstore:Option[DataStore]=None
  private var _tempFile:Option[File]=None
  private val _extensions  = "shp" :: "shx" :: "dbf" :: "prj" :: Nil

  def getSchema(/*shapefile:File*/):StructType = {
    val shapefile = _tempFile match { case Some(s) => s case None => downloadTempCopies}
    val map = new java.util.HashMap[String, URL]()
    map.put("url", shapefile.toURI().toURL());
    val store = _dbstore match { case Some(s) => s case None => { val x = DataStoreFinder.getDataStore(map); _dbstore = Some(x); x}}
    
    val fields = store.getSchema(store.getNames.get(0).getURI).getTypes().toList.flatMap(attr => 
      attr.getBinding match {
        case x if(classOf[java.lang.String].isAssignableFrom(x)) => StructField(attr.getName.getURI, StringType, false) :: Nil
        case x if(classOf[java.lang.Integer].isAssignableFrom(x)) => StructField(attr.getName.getURI, IntegerType, false) :: Nil
        case x if(classOf[java.lang.Double].isAssignableFrom(x)) => StructField(attr.getName.getURI, DoubleType, false) :: Nil
        case x if(classOf[java.lang.Float].isAssignableFrom(x)) => StructField(attr.getName.getURI, FloatType, false) :: Nil
        case x if(classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(x)) => ( 
          StructField("GeometryBinary", BinaryType, false) 
          :: Nil
        )
        case _ => StructField(attr.getName.getURI, StringType, false) :: Nil
      }
    )
    return StructType(StructField("ID", StringType, false)::fields)   
  }   

  def downloadTempCopies():File = { 
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration 
    val fs = FileSystem.get( hadoopConf )
    val path_s = shapeHDFSfile.toString 
    val uhome = System.getProperty("user.home")
    
    val bsource = path_s.substring(0, path_s.size-4)
    val tmpDir = new File(uhome);
    val tmpBase = s"$uhome/${RandomStringUtils.randomAlphanumeric(8)}";
           
    //Downloading temporary files
    _extensions.foreach(ext => {
      //Importing file from hdfs
      val source = new Path(s"$bsource.$ext")
      val tempdest= s"$tmpBase.$ext" 
      if(!fs.exists(source)) {
        println(s"Cannot find file $source")
      }
      else {
        val in = fs.open(source);
        val tempout = new FileOutputStream(new File(tempdest))
        try {
          val buffer = new Array[Byte](1024*512);
          var bytesRead = -1;
          while ({bytesRead = in.read(buffer);bytesRead != -1})
             tempout.write(buffer, 0, bytesRead);
          println(s"File $source  write to $tempdest completed")
        } finally {
          tempout.close()
          in.close();
        }
      }
    })
    val f = new File(s"$tmpBase.${_extensions(0)}")  
    _tempFile = Some(f)
    return f
  }

  def deleteTempCopies() {
    _tempFile match { 
      case Some(f) => { 
        val tmpBase = f.toString.substring(0, f.toString.size - 4)
        _extensions.foreach(ext => {
          val tmp= new File(s"$tmpBase.$ext")
          if(tmp.exists) tmp.delete()
        })
      }
      case None => println("No temp files to delete")
    }
  }
  def write2parquet(destination:Path, append:Boolean=false, batchSize:Integer= 5000) = {
    val schema = getSchema() 
    val shapefile = _tempFile match { case Some(s) => s case None => downloadTempCopies}
 
    val map = new java.util.HashMap[String, URL]()
    map.put("url", shapefile.toURI().toURL());
    val store = _dbstore match { case Some(s) => s case None => { val x = DataStoreFinder.getDataStore(map); _dbstore = Some(x); x}}
    val featureSource = store.getFeatureSource(store.getTypeNames()(0))   
    val collection = featureSource.getFeatures();
    val iterator = collection.features();
    val rowBuf = scala.collection.mutable.ArrayBuffer.empty[Row]
    var i = 0
    var partition = 0
    while(iterator.hasNext) {
      val row = iterator.next
      val attr = row.getAttributes()
      rowBuf += Row.fromSeq(
        (Seq(row.getID) 
          ++ attr.toSeq)
        .flatMap(e => e match { 
          case x:java.lang.String => e :: Nil
          case x:java.lang.Integer => e :: Nil
          case x:java.lang.Double => e :: Nil
          case x:java.lang.Float => e :: Nil
          case x:com.vividsolutions.jts.geom.Geometry =>  
            (serializeToByteArray(e) 
            :: Nil)
          case _ => e.toString :: Nil
        }) 
      )
      if(i % batchSize == batchSize -1) {
        val dataRDD = spark.sparkContext.parallelize(rowBuf)
        val encoded = spark.createDataFrame(dataRDD, schema)
        encoded.write.mode(if(partition==0 && !append) "Overwrite" else "Append").parquet(destination.toString)
        println(s"Partition $partition has been written with ${rowBuf.size} rows")
        rowBuf.clear
        partition = partition + 1
      }
      i = i + 1
    }
    if(rowBuf.size>0) {
      val dataRDD = spark.sparkContext.parallelize(rowBuf)
      val encoded = spark.createDataFrame(dataRDD, schema)
      encoded.write.mode(if(partition==0 && !append) "Overwrite" else "Append").parquet(destination.toString)
      println(s"Partition $partition has been written with ${rowBuf.size} rows")
      rowBuf.clear
    }
    iterator.close
  }
  def serializeToByteArray(obj:Object):Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(obj)
    return b.toByteArray()
  }

}
  



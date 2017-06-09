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
import org.geotools.referencing.CRS;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.geotools.geometry.jts.JTS;
import org.opengis.referencing.operation.MathTransform

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
    val fileschema = store.getSchema(store.getNames.get(0).getURI)

    val fields = fileschema.getTypes().toList.flatMap(attr => 
      attr.getBinding match {
        case x if(classOf[java.lang.String].isAssignableFrom(x)) => StructField(attr.getName.getURI, StringType, false) :: Nil
        case x if(classOf[java.lang.Integer].isAssignableFrom(x)) => StructField(attr.getName.getURI, IntegerType, false) :: Nil
        case x if(classOf[java.lang.Double].isAssignableFrom(x)) => StructField(attr.getName.getURI, DoubleType, false) :: Nil
        case x if(classOf[java.lang.Float].isAssignableFrom(x)) => StructField(attr.getName.getURI, FloatType, false) :: Nil
        case x if(classOf[com.vividsolutions.jts.geom.Geometry].isAssignableFrom(x)) => ( 
          StructField("GeometryBinary", BinaryType, false) 
          :: StructField("MaxLat", DoubleType, false) 
          :: StructField("MinLat", DoubleType, false) 
          :: StructField("MaxLong", DoubleType, false) 
          :: StructField("MinLong", DoubleType, false) 
          :: StructField("BaseCoordinateSystem", StringType, false) 
          :: StructField("TransformedToWGS84", BooleanType, false) 
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
    try { 
      val map = new java.util.HashMap[String, URL]()
      map.put("url", shapefile.toURI().toURL());
      val store = _dbstore match { case Some(s) => s case None => { val x = DataStoreFinder.getDataStore(map); _dbstore = Some(x); x}}

      //Trying to obtain the with CRS (Coordinate Reference System)
      var baseCRSCode:Option[String] = None
      var baseCRS:Option[CoordinateReferenceSystem] = None
      var transformation:Option[MathTransform] = None
      val fileschema = store.getSchema(store.getNames.get(0).getURI)
      (fileschema.getCoordinateReferenceSystem match { case x:CoordinateReferenceSystem => Some(x) case _=> None})
        .foreach(fCRS => {
          var fileCRS = fCRS
          CRS.lookupIdentifier(fileCRS, true ) match {
            case matchedCode:String => {
              baseCRSCode = Some(matchedCode)
              try { 
                CRS.decode(matchedCode) match { 
                  case x:CoordinateReferenceSystem => 
                    fileCRS = x 
                  case _ =>  
                    println("Could not obtain better CRS from Projection code")  
                }
              }
              catch { case e:Exception => println("Could not decode the CRS")  }
            }
            case _ => println("Could not find code from CRS")
          }
          val geoCRS = org.geotools.referencing.crs.DefaultGeographicCRS.WGS84;
          try { transformation = Some(CRS.findMathTransform(fileCRS, geoCRS, true))}
          catch { case e:Exception =>println("Cannot write transformatiion for CRS") }
          baseCRS = Some(fileCRS)
        })

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
            case g:com.vividsolutions.jts.geom.Geometry => {
              //Here we are received the geometry object. This block will return a list the list of cells related with it
              //If a transformation to longitud and latitud was found we transform it 
              var geom = transformation match {
                case Some(t) => JTS.transform(g, t)
                case None => g
              }
              //We calculate the bounding box point
              val coords = geom.getCoordinates()
              val maxx = coords.map(c => c.x).reduce((x0,x1)=>if(x0>x1) x0 else x1)
              val minx = coords.map(c => c.x).reduce((x0,x1)=>if(x0<x1) x0 else x1)
              val maxy = coords.map(c => c.y).reduce((y0,y1)=>if(y0>y1) y0 else y1)
              val miny = coords.map(c => c.y).reduce((y0,y1)=>if(y0<y1) y0 else y1)

              var maxlat = maxy
              var minlat = miny
              var maxlong = maxx
              var minlong = minx
              
              // If a CRS was found we define the set of longitud and latitud points on a meaningful way
              baseCRS match {
                case Some(crs) =>
                  if( CRS.getAxisOrder(crs) == CRS.AxisOrder.LAT_LON){
                    maxlat = maxx
                    minlat = minx
                    maxlong = maxy
                    minlong = miny
                  }
                case _ => {}
              }

              //Now we have everything we need, we can start returning the row
              ( 
		//Geometry columns
                //1. The serialized binary Object
                serializeToByteArray(geom)
                //2. The array of latitude coordinates
                :: maxlat
                :: minlat
                :: maxlong
                :: minlong
                //3. Coordinate system information
                :: (baseCRSCode match {case Some(s)=>s case _=>"Unknown" })
                :: (transformation match {case Some(s)=>true case _=> false })
                :: Nil
              )
            }
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
    } finally { 
      deleteTempCopies() 
    }
  }
  def serializeToByteArray(obj:Object):Array[Byte] = {
    val b = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(b)
    o.writeObject(obj)
    return b.toByteArray()
  }

}
  



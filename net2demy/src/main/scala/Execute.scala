package demy.webReader
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, GlobFilter}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import java.io.File
import java.io.BufferedReader;
import java.io.BufferedInputStream;
import java.io.InputStreamReader;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URL
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import org.apache.commons.compress.archivers.sevenz.SevenZFile
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import java.nio.channels.Channels
import org.apache.commons.compress.compressors.CompressorStreamFactory

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;

import org.jsoup.Jsoup
import org.jsoup.nodes.Element


import demy.geo.GeoManager

object Execute {
  def main(args: Array[String]) {
    val spark = SparkSession
     .builder()
     .appName("net2demy")
     .getOrCreate()
    val sql = spark.sqlContext
    val sc = spark.sparkContext
    val hadoopConf = sc.hadoopConfiguration 

    import sql.implicits._
    import org.apache.spark.sql.types._

    val proxyHost = sc.textFile("hdfs:///spark/twitter/ProxyHost").collect()(0)
    val proxyPort = sc.textFile("hdfs:///spark/twitter/ProxyPort").collect()(0).toInt
 
    val url2get = spark.read.json("hdfs:///data/source2demy/web2demy/links-to-import.json")
    url2get.collect.foreach(row => {
      val check = row.getAs[String]("check")
      val action_type = row.getAs[String]("type")
      val conf_dest = row.getAs[String]("dest")
      val name = row.getAs[String]("name")
      val active = row.getAs[Long]("active")
      val policy = row.getAs[String]("policy")
      val post = row.getAs[String]("post")
      val conf_url = row.getAs[String]("href")
      val crawling_filter = row.getAs[String]("crawling-filter")
      val crawling_matching = row.getAs[String]("crawling-matching")
      val crawling_contentType = row.getAs[String]("crawling-contentType")
      val crawling_keep_after = row.getAs[String]("crawling-keep-after")
      val crawling_deflate = row.getAs[String]("crawling-deflate")=="true"

      val links = 
           if(active > 0 && action_type == "direct-download") Map((conf_url, conf_dest))
           else if(active > 0){
             println(s"About to crawl $name")
             val filter = crawling_filter.r
             val matching = crawling_matching.r
             val contentType = crawling_contentType.r
             val startUrl = conf_url 

             val toVisit = scala.collection.mutable.Set(startUrl)
             val toDownload = scala.collection.mutable.Set[String]()
             val visited = scala.collection.mutable.Set[String]()
             if(toVisit.size > 0) println(s"Going to visit: ${toVisit.head}")
             while(toVisit.size > 0) {
                 val url = toVisit.head
                 if(!visited(url)) {
                     val resp = Jsoup.connect(url).proxy(proxyHost, proxyPort).timeout(10*1000).ignoreContentType(true).userAgent("Mozilla").execute();
                     //println(s"${resp.contentType()} ${resp.statusCode()}: ${resp.statusMessage()}")
                     val foundUrl = (contentType.findFirstIn(resp.contentType()) match {case Some(s) => Some(resp.parse()) case _ => None}).toList
                         .flatMap(doc => doc.select("a").toArray)
                         .flatMap(a => a match { case e:Element => Some(e.absUrl("href")) case _ => None })
                         .filter(a => !visited(a))
                         .flatMap(a => filter.findFirstIn(a) match { case Some(s) => Some(a) case _ =>None })
                         .toSet
                     toDownload ++= foundUrl.flatMap(a => matching.findFirstIn(a) match { case Some(s) => Some(a) case _ =>None })
                     toVisit ++= (foundUrl) //&~ toDownload) 
                 }
                 toVisit -= url
                 visited += url
                 println(s"Visited: ${visited.size} ToVisit: ${toVisit.size} ToDownLoad: ${toDownload.size}")
             }
             toDownload.map(p => (p, p.replace(conf_url, conf_dest) 
                                     match { 
                                       case s => "\\.zip$|\\.7z$|\\.tar$\\.gz|.bz2$".r.findFirstIn(s) 
                                        match { 
                                         case Some(ext) => s.slice(0, s.size - (if(crawling_deflate) ext.size else 0))
                                         case None => s
                                        }
                                      }
                           )).toMap
           }
           else {
              println(s"About to download $name")
              Map[String, String]()
           }
      links.foreach(p => p match { case (raw_url, dest) => {
        var downloaded = false
        var remoteFootprint=""
          if(policy == "head-check") {
          val currentFootprint = getCacheFootprint(dest, hadoopConf)
          val remoteFootprint = getRemoteFootprint(raw_url, dest, proxyHost, proxyPort)
          if(currentFootprint != remoteFootprint) {
            println("Cache has changed, downloading file")
            net2hdfs(raw_url, dest, proxyHost, proxyPort, hadoopConf)          
            downloaded = true
          } else {
            println("Cache is the same skipping download")
          }
        } else if(policy == "immutable") {
          if(!fileExists(dest, hadoopConf)) {
            println("File does no exists, downloading file")
            net2hdfs(raw_url, dest, proxyHost, proxyPort, hadoopConf)          
            downloaded = true
          } else {
            println("Cache is the same skipping download")
          }
        }
        if(downloaded) {
          if(post == "json2parquet") {
            val json = spark.read.json(dest)
            json.write.mode("Overwrite").parquet(s"$dest.parquet")
          }
          else if(post == "processBAN") {
            println("Writing BAN to parquet")
            val schema = StructType (
              StructField("id", StringType, true) ::
              StructField("nom_voie", StringType, true) ::
              StructField("id_fantoir", StringType, true) ::
              StructField("numero", StringType, true) ::
              StructField("rep", StringType, true) ::
              StructField("code_insee", StringType, true) ::
              StructField("code_post", StringType, true) ::
              StructField("alias", StringType, true) ::
              StructField("nom_ld", StringType, true) ::
              StructField("nom_afnor", StringType, true) ::
              StructField("libelle_acheminement", StringType, true) ::
              StructField("x", DoubleType, true) ::
              StructField("y", DoubleType, true) ::
              StructField("lon", DoubleType, true) ::
              StructField("lat", DoubleType, true) ::
              StructField("nom_commune", StringType, true) :: Nil
             )

             val csv = spark.read.schema(schema).option("header", "true").option("header", "true").option("sep",";").csv(s"$dest/BAN_licence_gratuite_repartage_*.csv")
             csv.write.mode("Overwrite").parquet(s"$dest.parquet")
             println(s"Parquet BAN succesfully written on $dest.parquet")
          }
          else if(post == "processContours") {
            val fs = FileSystem.get(hadoopConf)
            val shapes_p = new Path(dest)
            val parquet_p = new Path(s"$dest.parquet")
            val filter = new GlobFilter("*.shp")

            //Deleting pqraiet file if exists already
            if(fs.exists(parquet_p))
              fs.delete(parquet_p, true)
            //Transforming downloaded shape files to a single parquet directory
            fs.listStatus(shapes_p, filter).foreach(f => {
              new GeoManager(f.getPath, spark).write2parquet(parquet_p, true)
            }) 
          }
          else if(post != null && post.startsWith("excludeLines:")) {
            val command = post.replace("excludeLines:", "")
            val contentExclude = if(command.contains(">>") ) command.substring(0, command.lastIndexOf(">>")) else command
            val fileFilter = if(command.contains(">>") ) command.substring(command.lastIndexOf(">>")+2) else ".*"

            val fs = FileSystem.get(hadoopConf)
            val destPath = new Path(dest)
            val filter = new GlobFilter(fileFilter)
            fs.listStatus(destPath, filter).foreach(f => {
              if(f.isFile) {
        	val in = new BufferedReader(new InputStreamReader(fs.open(f.getPath),"UTF-8"));
                val out = new BufferedWriter(new OutputStreamWriter(fs.create(f.getPath.suffix(".tmp"), true), "UTF-8"))
                println(s"Excluding:->${contentExclude}<--")
                Iterator.continually(in.readLine()).takeWhile(_ != null)
        	  .filter(s => !s.matches(contentExclude)) 
        	  .foreach(s => {out.write(s"$s\n")}) 
                out.close()
                in.close()
                fs.delete(f.getPath)
                fs.rename(f.getPath.suffix(".tmp"), f.getPath)
              }
            })
          }
          if(policy == "head-check") {
            setCacheFootprint(dest, remoteFootprint, hadoopConf ) 
          }
        }
      }})
    })
    spark.stop
  }

  def fileExists(dest:String, conf:org.apache.hadoop.conf.Configuration):Boolean = {
      val path = new Path(s"hdfs://$dest") 
      val fs = FileSystem.get( conf )
      return fs.exists(path)
  }
  def getCacheFootprint(dest:String, conf:org.apache.hadoop.conf.Configuration):String = {
      val path = new Path(s"hdfs://$dest.cache") 
      //trying to get current cache
      val fs = FileSystem.get( conf )
      if(!fs.exists(path))
        return "";
      val in = new BufferedReader(new InputStreamReader(fs.open(path)));
      val cache = in.readLine();
      in.close()
      println(s"Cache found! $cache");
      return cache;       
  }
  def setCacheFootprint(dest:String, footprint:String, conf:org.apache.hadoop.conf.Configuration) {
      val path = new Path(s"hdfs://$dest.cache") 
      //trying to get current cache
      val fs = FileSystem.get( conf )
      val out = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)))
      out.write(footprint);
      out.close()
      println(s"""Cache written! $footprint""");
  }
  def getRemoteFootprint(raw_url:String, dest:String, proxyHost:String, proxyPort:Int):String  = {
    val url = new URL(raw_url);
    val host = url.getHost
    val protocol = url.getProtocol
    val port = if(url.getPort == -1 && protocol == "http")  80 else if(url.getPort == -1 && protocol == "https")  443 else  url.getPort
    val file = url.getFile

    var footprint:String = "Not Found"
    val httpclient = HttpClients.createDefault();
    try {
      val target = new HttpHost(host, port, protocol);
      val proxy = new HttpHost(proxyHost, proxyPort, "http");
      val config = RequestConfig.custom()
              .setProxy(proxy)
              .build();
      val request  = new HttpHead(file);
      request.setConfig(config);

      println("Trying to get head from" + request.getRequestLine() + " to " + target + " via " + proxy);
      val response = httpclient.execute(target, request);
      try {
        println("----------------------------------------");
        var etag=""
        var datemodified=""
        var size=""
        var found = false
        val headers = response.getAllHeaders()
        headers.foreach(e => {
          if(e.getName == "Last-Modified") {datemodified=e.getValue; found=true;}
          if(e.getName == "ETag") {etag=e.getValue; found=true;}
          if(e.getName == "Content-Length") {size=e.getValue; found=true;}
        });
        if(found) {
          footprint = s"""{\"Last-Modified\":\"$datemodified\", \"ETag\":$etag, \"Content-Length\":\"$size\" }"""
          println(s"Footprint is: $footprint")
        }
      } finally {
          response.close();
      }
    } finally {
        httpclient.close();
    }
    return footprint
  }

  def net2hdfs(raw_url:String, dest:String, proxyHost:String, proxyPort:Int, conf:org.apache.hadoop.conf.Configuration):String  = {
    val url = new URL(raw_url);
    val host = url.getHost
    val protocol = url.getProtocol
    val port = if(url.getPort == -1 && protocol == "http")  80 else if(url.getPort == -1 && protocol == "https")  443 else  url.getPort
    val filepath = url.getFile

    var footprint:String = "Not Found"
    val httpclient = HttpClients.createDefault();
    try {
      println(s"Trying to get file from $raw_url");
      val target = new HttpHost(host, port, protocol);
      val proxy = new HttpHost(proxyHost, proxyPort, "http");
      val config = RequestConfig.custom()
              .setProxy(proxy)
              .build();
      val request  = new HttpGet(filepath);
      request.setConfig(config);

      println("Trying to get file from" + request.getRequestLine() + " to " + target + " via " + proxy);
      val response = httpclient.execute(target, request);
      val fs = FileSystem.get( conf )
      try {
        val file  = response.getEntity()
        if (file != null) {
          var instream = file.getContent();
          //if fil us a 7zip file that should be decompressed we will do it on a directory, passing trhough a local copy
          if(filepath.contains(".7z") && !dest.contains(".7z")) {
            val uhome = System.getProperty("user.home")
            val temp_f =  File.createTempFile(".7z-demy", "tmp", new File(uhome))
            val tempout = new FileOutputStream(temp_f)
            var totalRead:Long = 0
            var megas = 0
            try {
               val buffer = new Array[Byte](1024*512);
               var bytesRead = -1;
               while ({bytesRead = instream.read(buffer);bytesRead != -1})
               {  
                  tempout.write(buffer, 0, bytesRead);
                  totalRead = totalRead + bytesRead
                  if(totalRead / (1024*1024) > megas + 10 ) {
                    megas = (totalRead / (1024*1024)).toInt
                    println(s"$megas mb written")
                  }
               }
            } finally {
              tempout.close()
              instream.close();
            }
            println(s"File written to temporary storage, start deflating")
  
            val path = new Path(s"hdfs://$dest") 
            //We delete the destination directory if exists
            if(fs.exists(path)) {
              fs.delete(path, true)
            }
            //Creating an empty directory
            fs.mkdirs(path)
            val sevenZFile = new SevenZFile(temp_f);
            try {
              var entry:SevenZArchiveEntry=null;
              totalRead = 0
               megas = 0
              // while there are entries I process them
              while ({entry = sevenZFile.getNextEntry(); entry != null}) {
                val entryname = entry.getName();
                val zname = entry.getName().replaceAll("/","-");
                if(!entryname.endsWith("/")) {
                  println(s"Found file $zname")
                  val zpath = new Path(s"hdfs://$dest/$zname") 
                  val out = fs.create(zpath, true)
                  try {
                    val buffer = new Array[Byte](1024*512);
                    var bytesRead = -1;
                    while ({bytesRead = sevenZFile.read(buffer);bytesRead != -1})
                    {
                       out.write(buffer, 0, bytesRead);
                       totalRead = totalRead + bytesRead
                       if(totalRead / (1024*1024) > megas + 10 ) {
                         megas = (totalRead / (1024*1024)).toInt
                         println(s"$megas uncompressed and written 2 hdfs")
                       }
                    }
                  } finally {
                    out.close()
                  }

                }
              }
            } finally {
              sevenZFile.close();
              temp_f.delete()
            }
          }
          //if fil us a zip file that should be decompressed we will do it on a directory
          else if(filepath.contains(".zip") && !dest.contains(".zip")) {
            val path = new Path(s"hdfs://$dest") 
            //We delete the destination directory if exists
            if(fs.exists(path)) {
              fs.delete(path, true)
            }
            //Creating an empty directory
            fs.mkdirs(path)
            val zis = new ZipInputStream(instream)
            try {
              var entry:ZipEntry=null;
              var totalRead:Long = 0
              var megas = 0
              // while there are entries I process them
              while ({entry = zis.getNextEntry(); entry != null}) {
                val entryname = entry.getName();
                val zname = entry.getName().replaceAll("/","-");
                if(!entryname.endsWith("/")) {
                  println(s"Found file $zname")
                  val zpath = new Path(s"hdfs://$dest/$zname") 
                  val out = fs.create(zpath, true)
                  try {
                    val buffer = new Array[Byte](1024*512);
                    var bytesRead = -1;
                    while ({bytesRead = zis.read(buffer);bytesRead != -1})
                    {
                       out.write(buffer, 0, bytesRead);
                       totalRead = totalRead + bytesRead
                       if(totalRead / (1024*1024) > megas + 10 ) {
                         megas = (totalRead / (1024*1024)).toInt
                         println(s"$megas mb written")
                       }
                    }
                  } finally {
                    out.close()
                  }

                }
              }
            } finally {
              zis.close();
              instream.close();
            }

          }
          else {
            //We are going to procedd with a single file (not a zip)
            //if files is bz2 we will uncompress on the fly
            if(filepath.contains(".bz2") && !dest.contains(".bz2")) {
              val bstream = new BufferedInputStream(instream);
              instream = new CompressorStreamFactory().createCompressorInputStream(bstream);
            }
            val path = new Path(s"hdfs://$dest") 
            val out = fs.create(path, true)
            try {
              val buffer = new Array[Byte](1024*512);
              var bytesRead = -1;
              var totalRead:Long = 0
              var megas = 0
              while ({bytesRead = instream.read(buffer);bytesRead != -1})
              {
                 out.write(buffer, 0, bytesRead);
                 totalRead = totalRead + bytesRead
                 if(totalRead / (1024*1024) > megas + 10 ) {
                   megas = (totalRead / (1024*1024)).toInt
                   println(s"$megas mb written")
                 }
              }
            } finally {
              instream.close();
              out.close()
            }
          }
          println("File written!");
        } else {
          println("File not found!");
        }
      } finally {
          response.close();
      }
    
    } finally {
        httpclient.close();
    }
    return footprint
  }
}

  



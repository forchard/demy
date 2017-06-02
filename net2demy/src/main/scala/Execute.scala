package net2demy
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import java.io.File
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpHead;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;

import net2demy._

object Execute {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Twitemy")
    val spark = new SparkContext(conf)
    val sql = new SQLContext(spark)

    import sql.implicits._

    val proxyHost = spark.textFile("hdfs:///spark/twitter/ProxyHost").collect()(0)
    val proxyPort = spark.textFile("hdfs:///spark/twitter/ProxyPort").collect()(0).toInt
 
    val url2get = sql.read.json("hdfs:///data/source2demy/web2demy/links-to-import.json")
    url2get.collect.foreach(row => {
      val check = row.getAs[String]("check")
      val dest = row.getAs[String]("dest")
      val name = row.getAs[String]("name")
      val policy = row.getAs[String]("policy")
      val raw_url = row.getAs[String]("href")
      val url = new URL(raw_url);
      val host = url.getHost    
      val protocol = url.getProtocol
      val port = if(url.getPort == -1 && protocol == "http")  80 else if(url.getPort == -1 && protocol == "https")  443 else  url.getPort
      val file = url.getFile

      val httpclient = HttpClients.createDefault();
      try {
          val target = new HttpHost(host, port, protocol);
          val proxy = new HttpHost("proxy.admin2.oxa.tld", 3128, "http");
          val config = RequestConfig.custom()
                  .setProxy(proxy)
                  .build();

          val request  = new HttpHead(file);
          request.setConfig(config);

          println("Executing request " + request.getRequestLine() + " to " + target + " via " + proxy);
          //http://stackoverflow.com/questions/9906003/incrementally-handling-twitters-streaming-api-using-apache-httpclient
          val response = httpclient.execute(target, request);
          try {
              println("----------------------------------------");
              println(response.getStatusLine());
              val reader = new BufferedReader(new InputStreamReader(response.getEntity().getContent()))
              var line:java.lang.String=null;
              while( {line = reader.readLine();  line!= null} ){
                 println(s"Line : $line")
              }
          } finally {
              response.close();
          }
      } finally {
          httpclient.close();
      }
    })


    spark.stop
  }
}



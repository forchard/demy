package twitemy
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

import twitter4j._
import twitemy._

object Execute {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Twitemy")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)
    import sql.implicits._

    val apiKey = sc.textFile("hdfs:///spark/twitter/ApiKey").collect()(0)
    val apiSecret = sc.textFile("hdfs:///spark/twitter/ApiSecret").collect()(0)
    val accessToken = sc.textFile("hdfs:///spark/twitter/AccessToken").collect()(0)
    val accessTokenSecret = sc.textFile("hdfs:///spark/twitter/AccessTokenSecret").collect()(0)
    val proxyHost = sc.textFile("hdfs:///spark/twitter/ProxyHost").collect()(0)
    val proxyPort = sc.textFile("hdfs:///spark/twitter/ProxyPort").collect()(0).toInt
    val track = sc.textFile("hdfs:///spark/twitter/Track").collect()
    val langs = sc.textFile("hdfs:///spark/twitter/Language").collect()
    val buffer = Array.fill[String](3000)("")  
    val blength = buffer.length
    val minChunk = 1000
    var readStart = 0
    var insertEnd = 0 
 
    object Util {
     val config = new twitter4j.conf.ConfigurationBuilder()
      .setOAuthConsumerKey(apiKey)
      .setOAuthConsumerSecret(apiSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)
      .setHttpProxyHost(proxyHost)
      .setHttpProxyPort(proxyPort)
      .setJSONStoreEnabled(true)
      .build

      def simpleStatusListener = new StatusListener() {
        def onStatus(status: Status) { bufferTweet(TwitterObjectFactory.getRawJSON(status)) }
        def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
        def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
        def onException(ex: Exception) { ex.printStackTrace }
        def onScrubGeo(arg0: Long, arg1: Long) {}
        def onStallWarning(warning: StallWarning) {}
      }

      def bufferTweet(tweet:String) {
        if(insertEnd + 1 % blength == readStart)
          println("Twit discarded, buffer is full");
        else {
          buffer(insertEnd) = tweet;
          insertEnd = (insertEnd + 1) % blength;
          println(s"$readStart >> $insertEnd")

        }
      }
    }

    val twitterStream = new TwitterStreamFactory(Util.config).getInstance
    twitterStream.addListener(Util.simpleStatusListener)
    val filter = new FilterQuery();
    filter.track(track:_*);
    filter.language(langs:_*);
    twitterStream.filter(filter)
    while(true) {
      val start = readStart
      val end = insertEnd
      var dist = end - start
      if(dist < 0)
        dist = (end + blength)-start
      if(dist > minChunk) {
        val cut = (start + minChunk - 1) % blength
        var toWrite = buffer.slice(start, if(cut<start) blength-1 else cut)
        if(cut<start) {
          toWrite = toWrite ++ buffer.slice(0, cut)
        }
        readStart = (start + minChunk) % blength
        println(s"$readStart >> $insertEnd")
        val rdd = sc.parallelize(toWrite, 1)
        var time = LocalDateTime.now;    
        val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH");
        val fileName = s"diabetes-twitter-${time.format(formatter)}";
        rdd.toDF.write.mode("append").format("text").option("compression", "gzip").save(s"hdfs:///data/twitter/diabetes/$fileName")
        println(s"Tweets written to $fileName")
      }    
      Thread.sleep(1000)
    }
    twitterStream.cleanUp
    twitterStream.shutdown

    sc.stop
  }
}



package twitemy
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import java.time.LocalDate
import java.time.format.DateTimeFormatter

/*import java.io.File
import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.http.entity.StringEntity;
*/
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
    val track = sc.textFile("hdfs:///spark/twitter/track").collect()
    val buffer = Array.fill[String](10000)("")  
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
        var time = LocalDate.now;    
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

/*    val httpclient = HttpClients.createDefault();
    val authorizationString = s"Bearer $accessToken";
    try {
        val target = new HttpHost("stream.twitter.com", 443, "https");
        val proxy = new HttpHost("proxy.admin2.oxa.tld", 3128, "http");

        val config = RequestConfig.custom()
                .setProxy(proxy)
                .build();

        val request  = new HttpPost("/1.1/statuses/filter.json");
        val postEntity = new StringEntity("track=diabetes","UTF-8");
        postEntity.setContentType("application/x-www-form-urlencoded")
        request.setEntity(postEntity)
        request.addHeader("Authorization", authorizationString);
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
    */


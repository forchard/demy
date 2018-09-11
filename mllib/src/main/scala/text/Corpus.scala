package demy.mllib.text

import demy.mllib.util.log
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.{Window}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path, GlobFilter}
import org.apache.spark.ml.feature.{Word2Vec, Word2VecModel}

case class Corpus(textSourcePath:String, word2VecPath:String, spark:SparkSession) {
    def fitWord2Vec() {
        //training word2vec

        val sql = spark.sqlContext
        import sql.implicits._
        val sc = spark.sparkContext
        val hadoopConf = sc.hadoopConfiguration
        val fs = FileSystem.get(hadoopConf)
        val textDir = new Path(textSourcePath)
        val files = fs.listFiles(textDir, true)    

        val textFiles = Iterator.continually(if(files.hasNext) files.next() else null).takeWhile(_ != null)
            .flatMap(f => if(f.getPath.getName == "_SUCCESS") None else Some(f.getPath.toString))
     
        val urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)"
        val textInput = spark.read.text(textFiles.toSeq :_*).as[String]
            .map(line => 
                line.toLowerCase
                    .replaceAll(urlPattern, " ")
                    .split("(?![0-9])[\\P{L}]|[\\(]|[\\)|]")
                    .map(w => Word.simplifyText(w).slice(0, 6))
                    .filter(w => w != "rt" && w.size > 0))
        .toDF("text")
        
        // Learn a mapping from words to Vectors.
        val word2Vec = new Word2Vec()
          .setInputCol("text")
          .setMaxIter(1)
          .setNumPartitions(1)
          .setOutputCol("result")
          .setVectorSize(300)
          .setMinCount(10)

        log.msg("fitting model" )
        val model = word2Vec.fit(textInput)


        // Save and load model
        log.msg("saving model")
        model.write.overwrite().save(word2VecPath)
    }
}
object Corpus {
  def main(args: Array[String]) {
    val spark = SparkSession
     .builder()
     .appName("Word2Vec in demy.mllib.text")
     .getOrCreate()

    if(args != null && args.size < 2){
      println("usage via spark submit jar [corpus path on hdfs] [model destination on hdfs]")
    }
    else {
      Corpus(textSourcePath=args(0), word2VecPath=args(1), spark = spark).fitWord2Vec()
    }
  }
}


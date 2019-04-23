package demy.mllib.test

import org.apache.spark.sql.SparkSession
import org.scalatest._
import demy.storage.Storage

object SharedSpark {
  var baseSpark:Option[SparkSession] = None
  def getSpark = baseSpark match { case Some(spark) => spark case _ => throw new Exception("spark context has not been initializedi yet")}
  def init { 
     baseSpark = Some(SparkSession.builder()
        .master("local[*]")
        //.config(conf)
        .appName("test")
        .getOrCreate())
     baseSpark.get.sparkContext.setLogLevel("WARN")
  }
  def stop { getSpark.stop }
}
class SparkTest  extends UnitTest with BeforeAndAfterAll
/*  with linalg.implicitsSpec
  with tuning.RandomSplitSpec
  with feature.ArrayHasherSpec
  with index.ImplicitsSpec
  with tuning.BinaryOptimalEvaluatorSpec
*/  with topic.TopicTreeSpec
{
  override def beforeAll() {
    SharedSpark.init
    println("Spark Session created!")
  }

  override def afterAll() {
    SharedSpark.stop
    Storage.getLocalStorage.removeMarkedFiles(cleanSandBox = true)
    Storage.getSparkStorage.removeMarkedFiles(cleanSandBox = true)
  }  
}


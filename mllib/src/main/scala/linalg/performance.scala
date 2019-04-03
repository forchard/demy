package demy.mllib.linalg

import demy.storage.{Storage}
import org.apache.spark.ml.linalg.{Vectors => SVects}
import breeze.linalg._
import demy.mllib.linalg.implicits._
import org.apache.spark.ml.linalg.VectorPub._
import demy.util.util
import org.apache.spark.sql.{SparkSession, Row, Dataset, DataFrame, Column}

object BLAS {
  lazy val i = com.github.fommil.netlib.BLAS.getInstance()
}
object performance {
  def time[R](block: => R): (R, Double) = {
      val t0 = System.currentTimeMillis()
      val result = block    // call-by-name
      val t1 = System.currentTimeMillis()
      (result, t1 - t0)
  }

  var vSize = 300
  var sampleSize = 10000
  val smallSeq = {
    import org.apache.spark.ml.linalg.{Vectors => SVects}
    val r = new scala.util.Random()
    Array.fill(sampleSize)((SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense, SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense))
  }

  def generateV(n:Int) = {
    Range(0, n).map{i => smallSeq(i%sampleSize)}
  }
  def testBLAS(spark:SparkSession, storage:Storage) {
    import spark.implicits._
    
    val bl = com.github.fommil.netlib.BLAS.getInstance()
    println(s"library found: $bl")
    //println(s"OPENBLAS_NUM_THREADS:=${sys.env("OPENBLAS_NUM_THREADS")}")

    var n = 16*16*16*32 //(131072)
    val r = new scala.util.Random
    val seq = Seq.fill(n)((SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense, SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense))
    
    var tf = 1000
    println(s"\nsinge thread Array")
    var t = time {seq.map{case(v1, v2) => (v1.asBreeze, v2.asBreeze) match {case (b1, b2) => b1.dot(b2)/(norm(b1)*norm(b2))}}.reduce(_ + _) }
    println(s"Breeze: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {seq.map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)}
    println(s"Demy: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {seq.map{case (v1, v2) => (v1.values, v2.values, vSize) match {case (a1, a2, l) => BLAS.i.ddot(l, a1, 1, a2, 1)/(BLAS.i.dnrm2(l, a1, 1)*BLAS.i.dnrm2(l, a2, 1))}}.reduce(_ + _) }
    println(s"Netlib: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
  
    

    println(s"\nsinge thread repeating range")
    t = time {generateV(n).map{ case(v1, v2) => (v1.asBreeze, v2.asBreeze) match {case (b1, b2) => b1.dot(b2)/(norm(b1)*norm(b2))}}.reduce(_ + _) }
    println(s"Breeze: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time { generateV(n).map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)}
    println(s"Demy ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time { generateV(n).map{ case (v1, v2) => (v1.values, v2.values, vSize) match {case (a1, a2, l) => BLAS.i.ddot(l, a1, 1, a2, 1)/(BLAS.i.dnrm2(l, a1, 1)*BLAS.i.dnrm2(l, a2, 1))}}.reduce(_ + _)}
    println(s"Netlib: ${n*tf/(t._2)} cossim/second ret:${t._1}") 

    val nPart = 16
    val node = storage.getTmpNode(Some("perf.test"))
    val dpart = Range(0, nPart).toSeq.toDS

    println(s"\nspark repeating range $nPart partitions")
    t = time {dpart.map(_ => generateV(n/nPart).map{ case(v1, v2) => (v1.asBreeze, v2.asBreeze) match {case (b1, b2) => b1.dot(b2)/(norm(b1)*norm(b2))}}.reduce(_ + _)).reduce(_ + _) }
    println(s"Breeze, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {dpart.map(_ => generateV(n/nPart).map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)).reduce(_ + _)}
    println(s"Demy, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {dpart.map{_ => 
      val bls = com.github.fommil.netlib.BLAS.getInstance()
      generateV(n/nPart).map{ case (v1, v2) => (v1.values, v2.values, vSize) match {case (a1, a2, l) => bls.ddot(l, a1, 1, a2, 1)/(bls.dnrm2(l, a1, 1)*bls.dnrm2(l, a2, 1))}}.reduce(_ + _)}.reduce(_ + _) }
    println(s"Netlib, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    
    val sseq = util.checkpoint(
      dpart
        .flatMap{i => 
          Seq.fill(n/nPart)((SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense, SVects.dense(Array.fill(vSize)(r.nextDouble)).toDense))
        }
        .repartition(nPart), node.path)
    println(s"\nspark stored $nPart partitions")
    t = time {sseq.map{case(v1, v2) => (v1.asBreeze, v2.asBreeze) match {case (b1, b2) => b1.dot(b2)/(norm(b1)*norm(b2))}}.reduce(_ + _) }
    println(s"Breeze, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {sseq.map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)}
    println(s"Demy, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {sseq.map{case (v1, v2) => (v1.values, v2.values, vSize) match {case (a1, a2, l) => BLAS.i.ddot(l, a1, 1, a2, 1)/(BLAS.i.dnrm2(l, a1, 1)*BLAS.i.dnrm2(l, a2, 1))}}.reduce(_ + _) }
    println(s"Netlib, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 

    val cseq = sseq.cache
    println(s"\nspark cached $nPart partitions")
    println(s"forcing cache sum is ${cseq.map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)}")
    t = time {cseq.map{case(v1, v2) => (v1.asBreeze, v2.asBreeze) match {case (b1, b2) => b1.dot(b2)/(norm(b1)*norm(b2))}}.reduce(_ + _) }
    println(s"Breeze, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {cseq.map{case(v1, v2) => v1.cosineSimilarity(v2)}.reduce(_ + _)}
    println(s"Demy, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
    t = time {cseq.map{case (v1, v2) => (v1.values, v2.values, vSize) match {case (a1, a2, l) => BLAS.i.ddot(l, a1, 1, a2, 1)/(BLAS.i.dnrm2(l, a1, 1)*BLAS.i.dnrm2(l, a2, 1))}}.reduce(_ + _) }
    println(s"Netlib, spark parallel: ${n*tf/(t._2)} cossim/second ret:${t._1}") 
  }
}

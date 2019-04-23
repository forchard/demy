package demy.mllib.test.tuning

import demy.mllib.test.{UnitTest, UnitTestVars}
import demy.mllib.test.SharedSpark
import demy.mllib.tuning.BinaryOptimalEvaluator
import scala.math.BigDecimal

object BinaryOptimalEvaluatorVars extends UnitTestVars {
  val pop = 460
  val poss = 120
  val negs = pop - poss // 340

  val baseT = 0.5
  val tpB = 100.0
  val fpB = 200.0
  val fnB = (poss - tpB) //20 
  val tnB = (negs - fpB) //140
  val precB = tpB / (tpB + fpB)//0.3333
  val recB = tpB / (tpB + fnB)//0.833
  val f1B = 2.0 * (precB * recB)/(precB + recB)//0.47


  val bestT =  0.75
  val tp = 90.0 //(100-10)
  val fp = 10.0  //(200-190)
  val fn = (poss - tp) //30 (20+10)
  val tn = (negs - fp) // 330 = 140 + 190
  val prec = tp / (tp + fp)//0.9
  val rec = tp / (tp + fn)//0.75
  val f1 = 2.0 * (prec * rec)/(prec + rec)//0.81

  val buckets = 5000

  def scoreLabelsDF =  {
    val spark = this.getSpark
    import spark.implicits._
    Range(0, buckets).reverse.toSeq.flatMap{i => //reverse is to test that our results are independent on order
      val p = i.toDouble/buckets.toDouble
      val j = i
      if(p < baseT) {
        //We put the expected base fn and tn from worst to best (fn, tn)
        if(j < fnB) {
          Some(p, 1.0)
        }
        else if(j - fnB < tnB)
          Some(p, 0.0)
        else None
      } else if(p < bestT) {
        //tn: here we have already tnB we need to include the rest of tn extTn = tn - tnB
        //fn: here we have already fnB we need to add extFn = fn - fnB
        val extTn = tn - tnB
        val extFn = fn - fnB
        //from worst to best (extFn, extTn)
        val j = i - (buckets * bestT).toInt + extTn + extFn
        if(j >= 0 && j < extFn) {
          Some(p, 1.0)
        }
        else if(j >= 0 && j - extFn < extTn)
          Some(p, 0.0)
        else None
      } else {
        //We put the expected tp and fp on the best model from best to worst
        val j = i - (buckets * bestT).toInt
        if(j < tp) {
          Some(p, 1.0)
        }
        else if(j - tp < fp)
          Some(p, 0.0)
        else None
      }
    }.toDF("score", "label")
  }

  lazy val getMetrics = {
    val spark = this.getSpark
    import spark.implicits._
     new BinaryOptimalEvaluator()
          .setOptimize("f1Score")
          .fit(scoreLabelsDF)
          .metrics
  }

  def r3(v:Double) = BigDecimal(v).setScale(3, BigDecimal.RoundingMode.HALF_UP).toDouble
  def round(v:Double, prec:Int) = BigDecimal(v).setScale(prec, BigDecimal.RoundingMode.HALF_UP).toDouble
}
trait BinaryOptimalEvaluatorSpec extends UnitTest {
  import demy.mllib.test.tuning.{BinaryOptimalEvaluatorVars => v}  
  "Optimal evaluator" should "get the right base precision, recall, f1Score" in {
    assert(
      (v.getMetrics match {case m => 
        (v.r3(m.basePrecision.get), v.r3(m.baseRecall.get), v.r3(m.baseF1Score.get))
      }) 
       == (v.r3(v.precB), v.r3(v.recB), v.r3(v.f1B))
    ) 
  }
  it should "get the right  threshold" in {
    assert((v.getMetrics match {case m => v.r3(m.threshold.get)}) == v.r3(v.bestT)) 
  }
  it should "get the right tp tn fp fn" in {
    assert((v.getMetrics match {case m => (m.tp.get, m.tn.get, m.fp.get, m.fn.get)}) == (v.tp.toInt, v.tn.toInt, v.fp.toInt, v.fn.toInt)) 
  }
}

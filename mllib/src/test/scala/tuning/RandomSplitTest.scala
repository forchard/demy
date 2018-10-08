package demy.mllib.test.tuning

import demy.mllib.test.UnitTest
import demy.mllib.test.SharedSpark
import demy.mllib.tuning.RandomSplit


trait RandomSplitSpec extends UnitTest{
  val rangeSize = 1000
  def rangeDS = {
    val spark = this.getSpark
    import spark.implicits._
    Range(0, rangeSize).toSeq.toDS
  }

  val foldRatio = 0.75
  val oneFoldSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(1)

  def singleSplitFolds = oneFoldSplit.buildFolds(rangeDS) match { case Array((train, test)) => Array(train.collect.toSet.asInstanceOf[Set[Int]], test.collect.toSet.asInstanceOf[Set[Int]])}

  val repeatedTries = 5
  def singleRepeatedFolds = {
    val folds = oneFoldSplit.buildFolds(rangeDS) 
    Range(0, repeatedTries)
      .flatMap(i =>folds)
      .map(p => (Array(p._1.collect.toSet.asInstanceOf[Set[Int]]), Array(p._2.collect.toSet.asInstanceOf[Set[Int]])))
      .reduce((p1, p2)=> (p1, p2) match {case ((trains1, tests1),(trains2, tests2))=>(trains1 ++ trains2, tests1 ++ tests2)})
  }
  "Random Split 1 fold" should "not mix rows between train and test" in {
    assert(singleSplitFolds.reduce((s1, s2)=>s1.intersect(s2)).size == 0) 
  }

  "Random Split 1 fold" should "not eliminate rows" in {
    assert(singleSplitFolds.reduce((s1, s2)=>s1.union(s2)).size == rangeSize) 
  }

  "Random Split 1 fold" should "always return same trainin/split set after buildFolds has been called" in {
    val (singleTrainSplits, singleTestSplits) = singleRepeatedFolds
    assert(Array(singleTrainSplits, singleTestSplits).map(splits => splits.reduce(_.union(_)) == splits(0)).reduce(_ && _)) 
  }
}

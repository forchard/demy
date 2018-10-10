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
  val numFoldsTest = 5

  val oneFoldSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(1)

  val multiFoldSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(5)

  def singleSplitFolds = oneFoldSplit.buildFolds(rangeDS) match { case Array((train, test)) => Array(train.collect.toSet.asInstanceOf[Set[Int]], test.collect.toSet.asInstanceOf[Set[Int]])}
  def multiSplitFolds = multiFoldSplit.buildFolds(rangeDS).map(p => p match {case (train, test) => (train.collect.toSet.asInstanceOf[Set[Int]], test.collect.toSet.asInstanceOf[Set[Int]])} )

  val repeatedTries = 5
  def singleRepeatedFolds = {
    val folds = oneFoldSplit.buildFolds(rangeDS) 
    Range(0, repeatedTries)
      .flatMap(i =>folds)
      .map(p => (Array(p._1.collect.toSet.asInstanceOf[Set[Int]]), Array(p._2.collect.toSet.asInstanceOf[Set[Int]])))
      .reduce((p1, p2)=> (p1, p2) match {case ((trains1, tests1),(trains2, tests2))=>(trains1 ++ trains2, tests1 ++ tests2)})
  }
  def multiRepeatedFolds = {
    val folds = multiFoldSplit.buildFolds(rangeDS) 
    Range(0, repeatedTries)
      .flatMap(i =>folds.zipWithIndex.map(t => t match {case ((train, test), i) =>(i, Array(train.collect.toSet.asInstanceOf[Set[Int]]), Array(test.collect.toSet.asInstanceOf[Set[Int]]))}))
      .groupBy(t => t match {case (i, rains, test) => i })
      .values.map(v => v.reduce((t1, t2) => (t1, t2) match {case ((i1, train1, test1), (i2, train2, test2)) => (i1, train1 ++ train2, test1 ++ test2) }))
      .map(t => t match {case (i, trains, tests) => (trains, tests) })
  }
  "Random Split 1 fold" should "not mix rows between train and test" in {
    assert(singleSplitFolds.reduce((s1, s2)=>s1.intersect(s2)).size == 0) 
  }
  it should "not eliminate rows" in {
    assert(singleSplitFolds.reduce((s1, s2)=>s1.union(s2)).size == rangeSize) 
  }
  it should "always return same trainin/split set after buildFolds has been called" in {
    val (singleTrainSplits, singleTestSplits) = singleRepeatedFolds
    assert(Array(singleTrainSplits, singleTestSplits).map(splits => splits.reduce(_.union(_)) == splits(0)).reduce(_ && _)) 
  }

  "Random Split multi fold"  should "produce the expected number of folds" in {
    assert(multiSplitFolds.size == numFoldsTest) 
  }
  it should "not mix rows between train and test" in {
    assert(multiSplitFolds.map(p => p match {case (train, test) =>train.intersect(test).size == 0 }).reduce(_ && _)) 
  }
  it should "not eliminate rows" in {
    assert(multiSplitFolds.map(p => p match {case (train, test) =>train.union(test).size == rangeSize }).reduce(_ && _)) 
  }
  it should "test all lines" in {
    assert(multiSplitFolds.map(p => p match {case (train, test) => test}).reduce(_.union(_)).size == rangeSize) 
  }
  it should "mantain the number of lines tested as the total number of lines" in {
    assert(multiSplitFolds.map(p => p match {case (train, test) => test.size}).reduce(_ + _) == rangeSize) 
  }
  it should "always return same trainin/split set after buildFolds has been called" in {
    assert(multiRepeatedFolds.map(p => p match {case (trains, tests) => trains.map(t => t == trains(0)).reduce(_ && _) && tests.map(t => t == tests(0)).reduce(_ && _)}).reduce(_ && _)) 
  }
}

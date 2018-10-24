package demy.mllib.test.tuning

import demy.mllib.test.UnitTest
import demy.mllib.test.SharedSpark
import demy.mllib.tuning.RandomSplit


trait RandomSplitSpec extends UnitTest{
  val rangeSize = 1000
  def rangeDS =  {
    val spark = this.getSpark
    import spark.implicits._
    Range(0, rangeSize).toSeq.toDS
  }

  val fixedSeed = 38L
  val groupByMod1 = 2
  val groupByMod2 = 7
  def groupByDS = {
    val spark = this.getSpark
    import spark.implicits._
    val m1 = groupByMod1
    val m2 = groupByMod2
    rangeDS.map(i => (i, i % m1, i % m2)).toDF("id", "col1", "col2")
  }
  val foldRatio = 0.75
  val numFoldsTest = 5

  val oneFoldSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(1)

  val oneFoldSplitSameSeed = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(1)
                  .setSeed(fixedSeed)

  val multiFoldSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(numFoldsTest)

  val multiFoldSplitSameSeed = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setNumFolds(numFoldsTest)
                  .setSeed(fixedSeed)

  val oneFoldGroupedSplit = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setGroupByCols(Array("col1", "col2"))
                  .setNumFolds(1)

  val oneFoldGroupedSplitSameSeed = new RandomSplit()
                  .setGroupByCols(Array[String]())
                  .setTrainRatio(foldRatio)
                  .setGroupByCols(Array("col1", "col2"))
                  .setNumFolds(1)
                  .setSeed(fixedSeed)

  def oneSplitFolds = {
    val spark = this.getSpark
    import spark.implicits._
    oneFoldSplit.buildFolds(rangeDS) match { case Array((train, test)) => Array(train.as[Int].collect.toSet, test.as[Int].collect.toSet)}
  }
  def oneSplitGroupedFolds = {
    val spark = this.getSpark
    import spark.implicits._
    oneFoldGroupedSplit.buildFolds(groupByDS) match { case Array((train, test)) => Array(train.as[(Int, Int, Int)].map(t => t._1).collect.toSet
                                                                                         ,test.as[(Int, Int, Int)].map(t => t._1).collect.toSet)}
  }
  def multiSplitFolds = {
    val spark = this.getSpark
    import spark.implicits._
    multiFoldSplit.buildFolds(rangeDS).map(p => p match {case (train, test) => (train.as[Int].collect.toSet, test.as[Int].collect.toSet)} )
  }
  val repeatedTries = 5
  def oneFoldRepeated = {
    val folds = oneFoldSplit.buildFolds(rangeDS) 
    Range(0, repeatedTries)
      .flatMap(i =>folds)
      .map(p => (Array(p._1.collect.toSet.asInstanceOf[Set[Int]]), Array(p._2.collect.toSet.asInstanceOf[Set[Int]])))
      .reduce((p1, p2)=> (p1, p2) match {case ((trains1, tests1),(trains2, tests2))=>(trains1 ++ trains2, tests1 ++ tests2)})
  }
  def oneFoldRepeatedSameSeed = {
    Range(0, repeatedTries)
      .flatMap(i =>oneFoldSplitSameSeed.buildFolds(rangeDS))
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
  def multiRepeatedFoldsSameSeed = {
    Range(0, repeatedTries)
      .flatMap(i =>multiFoldSplitSameSeed.buildFolds(rangeDS).zipWithIndex.map(t => t match {case ((train, test), i) =>(i, Array(train.collect.toSet.asInstanceOf[Set[Int]]), Array(test.collect.toSet.asInstanceOf[Set[Int]]))}))
      .groupBy(t => t match {case (i, rains, test) => i })
      .values.map(v => v.reduce((t1, t2) => (t1, t2) match {case ((i1, train1, test1), (i2, train2, test2)) => (i1, train1 ++ train2, test1 ++ test2) }))
      .map(t => t match {case (i, trains, tests) => (trains, tests) })
  }
  def oneFoldRepeatedGroupBySameSeed = {
    val spark = this.getSpark
    import spark.implicits._
    Range(0, repeatedTries)
      .flatMap(i =>oneFoldGroupedSplitSameSeed.buildFolds(groupByDS))
      .map(p => p match { case (train, test) => (Array(train.as[(Int, Int, Int)].map(t => t._1).collect.toSet)
                                               ,  Array(test.as[(Int, Int, Int)].map(t => t._1).collect.toSet))})
      .reduce((p1, p2)=> (p1, p2) match {case ((trains1, tests1),(trains2, tests2))=>(trains1 ++ trains2, tests1 ++ tests2)})
  }
  def oneFoldRepeatedGroupBy = {
    val spark = this.getSpark
    import spark.implicits._
    val folds = oneFoldGroupedSplit.buildFolds(groupByDS)
    Range(0, repeatedTries)
      .flatMap(i =>folds )
      .map(p => p match { case (train, test) => (Array(train.as[(Int, Int, Int)].map(t => t._1).collect.toSet)
                                               ,  Array(test.as[(Int, Int, Int)].map(t => t._1).collect.toSet))})
      .reduce((p1, p2)=> (p1, p2) match {case ((trains1, tests1),(trains2, tests2))=>(trains1 ++ trains2, tests1 ++ tests2)})
  }
  "Random Split 1 fold" should "not mix rows between train and test" in {
    assert(oneSplitFolds.reduce((s1, s2)=>s1.intersect(s2)).size == 0) 
  }
  it should "not eliminate rows" in {
    assert(oneSplitFolds.reduce((s1, s2)=>s1.union(s2)).size == rangeSize) 
  }
  it should "honor trainRatio" in {
    assert(Math.abs(foldRatio - oneSplitFolds(0).size.toDouble/rangeSize)<0.1) 
  }
  it should "always return same trainin/split set after buildFolds has been called" in {
    val (singleTrainSplits, singleTestSplits) = oneFoldRepeated
    assert(Array(singleTrainSplits, singleTestSplits).map(splits => splits.reduce(_.union(_)) == splits(0)).reduce(_ && _)) 
  }
  it should "always return same trainin/split when using same seed" in {
    val (singleTrainSplits, singleTestSplits) = oneFoldRepeatedSameSeed
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
  it should "honor trainRatio" in {
    assert(multiSplitFolds.map(p => p match {case (train, test) => Math.abs((numFoldsTest.toDouble-1.0)/numFoldsTest - train.size.toDouble/rangeSize)<0.1}).reduce(_ && _)) 
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
  it should "always return same trainin/split set when using same seed" in {
    assert(multiRepeatedFoldsSameSeed.map(p => p match {case (trains, tests) => trains.map(t => t == trains(0)).reduce(_ && _) && tests.map(t => t == tests(0)).reduce(_ && _)}).reduce(_ && _)) 
  }
  "Random Grouped Split 1 fold" should "not mix rows between train and test" in {
    assert(oneSplitGroupedFolds.reduce((s1, s2)=>s1.intersect(s2)).size == 0) 
  }
  it should "not eliminate rows" in {
    assert(oneSplitGroupedFolds.reduce((s1, s2) => s1.union(s2)).size == rangeSize) 
  }
  it should "honor trainRatio" in {
    assert(Math.abs(foldRatio - oneSplitGroupedFolds(0).size.toDouble/rangeSize)<0.15) 
  }
  it should "always return same trainin/split set after buildFolds has been called" in {
    val (singleTrainSplits, singleTestSplits) = oneFoldRepeatedGroupBy
    assert(Array(singleTrainSplits, singleTestSplits).map(splits => splits.reduce(_.union(_)) == splits(0)).reduce(_ && _)) 
  }
  it should "always return same trainin/split set when using same seed" in {
    val (singleTrainSplits, singleTestSplits) = oneFoldRepeatedGroupBySameSeed
    assert(Array(singleTrainSplits, singleTestSplits).map(splits => splits.reduce(_.union(_)) == splits(0)).reduce(_ && _)) 
  }
  it should "honor group by columns" in {
    assert(oneSplitGroupedFolds.map(set => set.toSeq.map(i => (i % groupByMod1, i % groupByMod2)).toSet).reduce(_.intersect(_)).size == 0) 
  }
}

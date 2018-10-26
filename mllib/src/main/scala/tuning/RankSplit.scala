package demy.mllib.tuning

import demy.mllib.params._
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame, Row, Column}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, abs, lit, dense_rank, sum, avg, count, not}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.ColumnName

class RankSplit(override val uid: String) extends /*Folder with*/ HasFolds with HasTrainRatio {
    final val rankingCols = new Param[Array[String]](this, "rankingCols", "Array indicating which columns are to be considered for ranking the lines")
    final val groupCols = new Param[Array[String]](this, "groupCols", "Array idicating rows to be treated as a group when folding (all rows with the same value will always go to the same fold)")
    final val aggregateAs = new Param[Array[String]](this, "aggregateAs", "If grouping, ranking cols should be aggregated. Possible values = 'sum', 'count', 'avg'")
    final val sortDescending = new Param[Array[Boolean]](this, "sortDescending", "Array indicating wether rank is don ascenting(true) or descending(false) for each ranking col (must have same length)")
    final val dropTempCols = new Param[Boolean](this, "dropTempCols", "If temporary columns (aggregates and ranking) should be droped from dataset")
    setDefault(trainRatio->0.75, dropTempCols-> true, groupCols->Array[String](), sortDescending->Array[Boolean](), aggregateAs -> Array[String]())
    def setRankingCols(value: Array[String]): this.type = set(rankingCols, value)
    def setGroupCols(value: Array[String]): this.type = set(groupCols, value)
    def setAggregateAs(value: Array[String]): this.type = set(aggregateAs, value)
    def setSortDescending(value: Array[Boolean]): this.type = set(sortDescending, value)
    def setDropTempCols(value: Boolean): this.type = set(dropTempCols, value)
    
    /*override */def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = {
      val nFolds = getOrDefault(numFolds)
      val groups = getOrDefault(groupCols)
      val aggregate = groups.size > 0
      val userRankCols = getOrDefault(rankingCols) 
      val aggAs = getOrDefault(aggregateAs).padTo(groups.size, "count")
      val dropTemp = getOrDefault(dropTempCols)

      //Step 1: defining measure columns (adding them as new if grouping) which values are going to produce the ranking
      val (measureCols, measureColNames) = ( 
        if(aggregate)
          (userRankCols.zip(aggAs).map(p => p match {
                                                     case (colName, "sum") => sum(col(colName)).over(Window.partitionBy(groups.map(gCol => col(gCol)):_*)).as(colName+"_"+this.uid)
                                                     case (colName, "avg") => avg(col(colName)).over(Window.partitionBy(groups.map(gCol => col(gCol)):_*)).as(colName+"_"+this.uid)
                                                     case (colName, "count") => count(col(colName)).over(Window.partitionBy(groups.map(gCol => col(gCol)):_*)).as(colName+"_"+this.uid)
                                                 })
           , userRankCols.map(c => c+"_"+this.uid))
        else 
          (userRankCols.map(c => col(c)), userRankCols) 
      )
      val dsWithMeasures = ds.select((Array(col("*")) ++ (if(aggregate) measureCols else Array[Column]())):_*)

      //Step 2: calculating the ranking by using the measures (and group keys when groupng)
      val sortDesc = getOrDefault(sortDescending).padTo(measureCols.size, true)
      val rankColName = "rank_"+this.uid 
      val rankCols = groups.map(c => col(c)) ++ measureCols.zip(sortDesc).map(p => p match {case (mCol, desc) => if(desc) mCol.desc else mCol.asc})
      val dsWithRanking = dsWithMeasures.withColumn(rankColName
                                             ,dense_rank().over(Window.orderBy(rankCols :_*))
                                         )
                                        .drop((if(aggregate && dropTemp ) measureColNames else Array[String]()):_*)
     
      //Step 3: Applying the folding by using the rank
      val probs = if(nFolds <= 1) Array(1 - getOrDefault(trainRatio), getOrDefault(trainRatio)) else Range(0, nFolds).map(_ => 1.0/nFolds).toArray

      val dist = distribution(probs = probs, tolerance = 0.01, maxSize = 100)
      val distSize = dist.map(p => p match {case (fold, positions) =>  positions.size}).reduce(_ + _)

      dist.map(p => p match {case (fold, positions) => { 
                        val Array(train, test) = Array(true, false).map(inTrain => {
                          dsWithRanking.filter(col(rankColName) % lit(distSize).isin(positions.map(p => lit(p)):_* ) match {case inFold => if(inTrain) not(inFold) else inFold })
                                       .drop((if(dropTemp) Array(rankColName) else Array[String]()):_*)
                        }) 
                        (train, test)
                    }})
    }

    def distribution(probs:Array[Double], tolerance:Double, maxSize:Int) = {
      val tol = tolerance
      var totalError = 1.0
      var sizes = probs.map(_ => 0)
      var i = 0
      Iterator.continually {
        val cRatios = if(i == 0) sizes.map(_.toDouble) else sizes.map(foldSize => foldSize.toDouble/(i)) 
        var foldToAdd = 0
        var biggestGap = 0.0
        val errors = probs.zip(cRatios).zipWithIndex.map(p => p match {case ((target, current), iFold) => {
            val error = target - current
            if(error > biggestGap) {
                foldToAdd = iFold
                biggestGap = error
            }
            error
        }})
        totalError = errors.reduce(Math.abs(_)+Math.abs(_))
        sizes(foldToAdd) = sizes(foldToAdd) + 1
        i = i + 1 
        (i, foldToAdd)
      }.takeWhile((_)=> (totalError > tol && i < maxSize)).toArray.groupBy(p => p._2).mapValues(a => a.map(p => p._1)).toArray.sortWith(_._1 < _._1)
    }

    def copy(extra: ParamMap): this.type = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("RankSplit"))
}

object RankSplit{
}

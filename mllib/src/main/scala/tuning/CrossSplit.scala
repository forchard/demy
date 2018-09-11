package demy.mllib.tuning

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._


class CrossSplit(override val uid: String) extends Folder {
      final val numFolds = new Param[Int](this, "numFolds", "The number of random folds to build")
          def setNumFolds(value: Int): this.type = set(numFolds, value)
              override def buildFolds(ds:Dataset[_]):Array[(Dataset[_], Dataset[_])] = {
                      val n = get(numFolds).get
                            val folds = ds.randomSplit(Range(0, n).toArray.map(i => 1.0/n))
                                  folds.zipWithIndex.map(p => p match {case (df, i) => (folds.zipWithIndex.filter(p => p._2 !=i).map(p=>p._1).reduce((df1, df2)=> df1.union(df2)), df)})
                                      }
                  def copy(extra: ParamMap): CrossSplit = {defaultCopy(extra)}    
                      def this() = this(Identifiable.randomUID("RandomSplit"))
}

object CrossSplit {}

package demy.mllib.evaluation

import demy.mllib.HasScoreCol
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.linalg.{Vector => MLVector, DenseVector}
import org.apache.spark.ml.attribute.AttributeGroup 
import org.apache.spark.sql.{Dataset, DataFrame}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, col, array}

class RawPrediction2Score(override val uid: String) extends Transformer with HasRawPredictionCol with HasScoreCol{
    def setRawPredictionCol(value: String): this.type = set(rawPredictionCol, value)
    override def transform(dataset: Dataset[_]): DataFrame = {
        val schema = dataset.schema
        val isArray = schema.apply(getOrDefault(rawPredictionCol)).dataType.getClass.getName match {
            case "org.apache.spark.ml.linalg.VectorUDT" => false
            case "org.apache.spark.sql.types.ArrayType" => true
            case other => throw new Exception("unsupported type for extracting score @epi")
        }
        dataset.withColumn(get(scoreCol).get, udf((rawPreds:Seq[MLVector])=>{
            val scores = rawPreds.map(rawPred => {
                var preds = rawPred.toArray
                if(preds.size!=2) throw new Exception("Score can only be calculated for binary class modles @epi")
                val Array(no, yes) = preds
                if(no>=0 && yes>=0) yes/(no + yes)
                else if(no>=0 && yes<=0) 0.5 - Math.atan(no-yes)/Math.PI
                else if(no<=0 && yes>=0) 0.5 + Math.atan(yes-no)/Math.PI
                else no/(no + yes)
            })
            new DenseVector(scores.toArray) 
        }).apply(if(isArray) col(get(rawPredictionCol).get) else array(col(get(rawPredictionCol).get))))
    }
    override def transformSchema(schema: StructType): StructType = {schema.add(new AttributeGroup(name=get(scoreCol).get).toStructField)}
    def copy(extra: ParamMap): RawPrediction2Score = {defaultCopy(extra)}    
    def this() = this(Identifiable.randomUID("RawPrediction2Score"))
}
object RawPrediction2Score {
}

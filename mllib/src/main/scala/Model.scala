package demy.mllib;

import demy.mllib.evaluation.{BinaryMetrics, HasBinaryMetrics}
import demy.mllib.util.log
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

case class Model(project:String, model:String, modelGroup:String, steps:Seq[ModelStep], source:DataFrame) {
    def getVersion(steps:String*) = {
        val stepSet = steps.toSet
        val notFounds = stepSet.diff(this.steps.map(s => s.name).toSet)
        if(notFounds.size>0)
            throw new Exception(s"The step(s) ${notFounds.mkString(",")} cannot be found on model")
        ModelVersion(steps = this.steps.flatMap(s => if(stepSet.contains(s.name)) Some(s) else None), comment = "")
    }
    def plan(baseModel:ModelVersion) = ModelPlan(base = baseModel)
    def run(plan:ModelPlan, logOn:Option[String]=None) = {
        plan.build().foreach(modelVersion => {
          modelVersion.printSchema()
          var binaryMetrics:Option[BinaryMetrics] = None
          var execMetrics = scala.collection.mutable.Map[String, Double]()
          val resdf = modelVersion.steps.foldLeft(this.source)((current, step) => {
              log.msg(s"Step ${step.name}")
              val (df, executedStep) = step.action match {
                  case t:Transformer => (t.transform(current), t)
                  case e:Estimator[_] => {
                      val model = e.fit(current)
                      (model.transform(current), model)
                  }
              }
              (logOn, executedStep) match {
                  case (Some(path), binEvaluator:HasBinaryMetrics) => binaryMetrics = Some(binEvaluator.metrics)
                  case _ =>{}
              }
              (logOn, executedStep) match {
                  case (Some(path), metricStep:HasExecutionMetrics) => execMetrics ++= metricStep.metrics.filter(p => metricStep.getLogMetrics && (metricStep.getMetricsToLog.size == 0 || metricStep.getMetricsToLog.contains(p._1))).map(p => (step.name+"_"+p._1, p._2)) 
                  case _ =>{}
              }
              if(step.show)
                  df.show
              df
          })
        
          logOn match {
              case Some(logPath) => {
                  var execRow = this.toRow(modelVersion)
                  execRow = binaryMetrics match {
                      case Some(metrics) => {
                          val mDF = source.sparkSession.createDataFrame(Seq(metrics))
                          new GenericRowWithSchema((execRow.toSeq ++ mDF.first.toSeq).toArray, StructType(execRow.schema.fields ++ mDF.schema.fields))
                      }
                      case _ => execRow
                  }
                  execRow = execMetrics.size match {
                      case 0 => execRow
                      case _ => {
                          val seq = execMetrics.toSeq
                          val names = seq.map(p => p._1)
                          val values =  seq.map(p => p._2)
                          new GenericRowWithSchema((execRow.toSeq ++ values).toArray, StructType(execRow.schema.fields ++ names.map(n => new StructField(name = n, dataType = DoubleType))))
                      }
                  }
                  source.sparkSession.createDataFrame(List(execRow.asInstanceOf[Row]).asJava, execRow.schema).write.mode("append").partitionBy("modelGroup", "project", "model").json(logPath)
              }
              case _ =>{}
          }
          resdf
        })
    }
    def toRow(comment:String):GenericRowWithSchema = new GenericRowWithSchema(values = Array(project, model, modelGroup, new java.sql.Timestamp(System.currentTimeMillis()), comment)
                                            ,schema = StructType(fields = Array(StructField(name="project", dataType=StringType)
                                                                                ,StructField(name="model", dataType=StringType)
                                                                                ,StructField(name="modelGroup", dataType=StringType)
                                                                                ,StructField(name="executedOn", dataType=TimestampType)
                                                                                ,StructField(name="comment", dataType=StringType)
                                                                                )))
    def toRow(version:ModelVersion):GenericRowWithSchema = {
        val modelRow = this.toRow(version.comment)
        val stepsToLog = version.steps.filter(step => step.log)
//                        .flatMap(p => p match {case (step, vIndex) => if(step.log) Some(step.versions(vIndex)) else None})
        val stepsValAndTypes = stepsToLog
                        .map(step => (step.version, StructField(name=step.family, dataType = StringType)))
                        
        val paramsValAndTypes = stepsToLog.flatMap(step => step.structFieldAndValuesToLog().map(sv => sv match {case (structField, value) => (value, structField) } ))

        val customLogs = version.customLogs
        val allValAndTypes = stepsValAndTypes ++ paramsValAndTypes
        var values:Seq[Any] = modelRow.toSeq
        values = values ++ customLogs.toSeq
        values = values ++ allValAndTypes.map(_._1).toSeq  
        var schema:Seq[StructField] = modelRow.schema.fields
        schema = schema ++ customLogs.schema.fields
        schema = schema ++ allValAndTypes.map(_._2) 
        new GenericRowWithSchema(values = values.toArray ,schema = new StructType(schema.toArray))
    }
}


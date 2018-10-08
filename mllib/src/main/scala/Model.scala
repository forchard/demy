package demy.mllib;

import demy.mllib.evaluation.{BinaryMetrics, HasBinaryMetrics}
import demy.mllib.util.log
import demy.mllib.params._
import org.apache.spark.ml.{Transformer, Estimator}
import org.apache.spark.ml.param.{Params}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col}
import org.apache.spark.sql.SparkSession
import scala.collection.JavaConverters._

case class Model(project:String, model:String, modelGroup:String, steps:Seq[ModelStep], snapshotPath:Option[String]) {
    def getVersion(steps:String*) = {
        val stepSet = steps.toSet
        val notFounds = stepSet.diff(this.steps.map(s => s.name).toSet)
        if(notFounds.size>0)
            throw new Exception(s"The step(s) ${notFounds.mkString(",")} cannot be found on model")
        ModelVersion(steps = this.steps.flatMap(s => if(stepSet.contains(s.name)) Some(s) else None), comment = "")
    }
    def fullVersion() = ModelVersion(steps = this.steps)
    def defaultVersion() = {
      ModelVersion(steps = {
          val iSteps = this.steps.zipWithIndex
          iSteps.flatMap(p => p match {case(step, i) => if(iSteps.filter(pp => pp match{ case (sstep, ii)=> step.name == sstep.name && step.version == sstep.version && ii<i}).size == 0)
                                                          Some(step)
                                                        else
                                                          None})
      })
    }
    def plan() = ModelPlan()
    def show(source:DataFrame, namedDataFrames:Map[String,DataFrame]= Map[String, DataFrame]()):Unit = {
      this.run(source = source, base = ModelVersion(this.defaultVersion.steps.map(s => s.option("show"->"true"))), logOn = None, namedDataFrames = namedDataFrames ) 
    }
    def show(source:DataFrame, steps:String*):Unit = this.show(source, Map[String, DataFrame](), steps:_*) 
    def show(source:DataFrame, namedDataFrames:Map[String, DataFrame], steps:String*):Unit = {
      val stepSet = steps.toSet
      this.run(source = source, base = ModelVersion(this.defaultVersion.steps.map(s => s.option("show"-> (if(stepSet.contains(s.name)) "true" else "false"))))
                ,  logOn = None, namedDataFrames = namedDataFrames) 
    }
    def run(source:DataFrame, plan:ModelPlan=ModelPlan(), base:ModelVersion=this.fullVersion, logOn:Option[String]=None
            , namedDataFrames:Map[String, DataFrame] = Map[String, DataFrame](), showSteps:Seq[String]=Seq[String](), stopAfter:Option[String]=None, maxVersions:Option[Int]=None) = {
        var i = 0
        val versions = plan.build(base, stopAfter) match {case vers =>  maxVersions match {case Some(max) => vers.take(max) case _ => vers}}

        versions.map(modelVersion => {
          //modelVersion.printSchema()
          log.msg(s"(${i}/${versions.size}:${Math.round(100.0* i/versions.size)}%) Startng Version: ${modelVersion.comment}")
          i = i + 1
          var binaryMetrics:Option[BinaryMetrics] = None
          var execMetrics = scala.collection.mutable.Map[String, Double]()
          var namedInputs = ((modelVersion.steps
                              .flatMap(s => s.input match {case Some(sName) => Some(sName -> None.asInstanceOf[Option[DataFrame]]) case _ => None})
                              .toMap) + ("#model"->Some(source))
                            ++ modelVersion.steps
                              .flatMap(s => s.paramInputs.filter(p => p._2.startsWith("$")).map(p => (p._2 -> None.asInstanceOf[Option[DataFrame]]))
                              .toMap) 
                            ++ namedDataFrames.map(p => p match {case (name, df) => ("#"+name, Some(df))})
                            )
          val resdf = modelVersion.steps.foldLeft(source)((current, step) => {
              log.msg(s"Step ${step.name}")
              val stepSource = step.input match {
                case Some(stepName) => 
                      namedInputs.get(stepName) match {
                          case Some(Some(df)) => df 
                          case _ => throw new Exception(s"Step $stepName has not yet been executed so its result cannot be used for step ${step.name}")
                      } 
                case _ => current
              }
              var theAction = step.paramInputs.foldLeft(step.action)((current, dfParam)
                    => step.action.set(step.action.getParam(dfParam._1), namedInputs.get(dfParam._2) match {
                        case Some(Some(df)) => df
                        case _ => throw new Exception(s"Cannot find the specified dataframe ${dfParam._2}")
                    })
                  )
              val (outDF, executedStep) = 
                (theAction, getStepSnapshot(modelVersion, step.name, stepSource.sparkSession))  match {
                  case (t, Some(snapshoted)) => (snapshoted, t)
                  case (t:Transformer, _) => (t.transform(stepSource), t)
                  case (e:Estimator[_], _) => {
                      val model = e.fit(stepSource)
                      (model.transform(stepSource), model)
                  }
                  case _ => throw new Exception("The current action type ${o.getClass.getName} is not supported @epi")
              }
              var df = if(step.select.size>0) outDF.select(step.select.map(s => col(s)):_*) else outDF
              df = if(step.drop.size>0) df.drop(step.drop:_*) else df
              df = step.renameCols.foldLeft(df)((current, p)=> current.drop(p._2).withColumnRenamed(p._1, p._2))
              //Caching or snapshoting the results step result dataframe if set
              df = (
                if(step.snapshot) {
                  this.setStepSnapshot(df, modelVersion, step.name) 
                } else if(step.cache) {
                  df.cache()
                } else {
                  df
                })

              //Storing stem result if used as named input on another step
              namedInputs.get("$"+step.name) match {
                case Some(s) => namedInputs = namedInputs + ("$"+step.name -> Some(df))
                case _ => {}
              }
              //Logging binary metrics uf set
              (logOn, executedStep) match {
                  case (Some(path), binEvaluator:HasBinaryMetrics) => binaryMetrics = Some(binEvaluator.metrics)
                  case _ =>{}
              }
              //Logging execution metrics uf set
              (logOn, executedStep) match {
                  case (Some(path), metricStep:HasExecutionMetrics) => execMetrics ++= metricStep.metrics.filter(p => metricStep.getLogMetrics && (metricStep.getMetricsToLog.size == 0 || metricStep.getMetricsToLog.contains(p._1))).map(p => (step.name+"_"+p._1, p._2)) 
                  case _ =>{}
              }
              //Showing  results if set
              if(step.show || showSteps.contains(step.name))
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

    def stepSnapshotPath(version:ModelVersion, stepName:String) = {
        this.snapshotPath match {
           case Some(lPath)=> lPath+"/"+this.project+"/"+this.model+"/"+stepName
           case _ => throw new Exception("Cannot snapshot since snapshot folder is not set @epi")
        }
    }
    def getStepSnapshot(version:ModelVersion, stepName:String, spark:SparkSession) = {
      val theStep = version.steps.filter(s => s.name == stepName).head
      if(theStep.reuseSnapshot) {
        val snapPath  = this.stepSnapshotPath(version, stepName)
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        if(fs.exists(new org.apache.hadoop.fs.Path(snapPath)))
          Some(spark.read.parquet(snapPath))
        else None
      } else {
        None
      } 
    }
    def setStepSnapshot(df:DataFrame, version:ModelVersion, stepName:String) = {
      val spark = df.sparkSession
      val theStep = version.steps.filter(s => s.name == stepName).head
      val snapPath  = this.stepSnapshotPath(version, stepName)
      val conf = spark.sparkContext.hadoopConfiguration
      val fs = org.apache.hadoop.fs.FileSystem.get(conf)
      if(!theStep.reuseSnapshot || !fs.exists(new org.apache.hadoop.fs.Path(snapPath)))
        df.write.mode("overwrite").parquet(snapPath)
      spark.read.parquet(snapPath)
    }
    def step(step:ModelStep):Model = Model(project = this.project, model = this.model, modelGroup=this.modelGroup, steps = this.steps :+ step, snapshotPath = this.snapshotPath)
    def step(name:String, action:Params, options:(String, String)*):Model = this.step(ModelStep(name = name, action = action).option(options:_*))
    def step(name:String, version:String, action:Params, options:(String, String)*):Model = this.step(ModelStep(name = name, version= version, family = name , action = action).option(options:_*))
    def snapshotPath(snapshotPath:String):Model =  Model(project = this.project, model = this.model, modelGroup=this.modelGroup, steps = this.steps, snapshotPath = Some(snapshotPath))
}
object Model {
   def apply(project:String, model:String, modelGroup:String):Model = Model(project = project, model = model, modelGroup = modelGroup, steps = Seq[ModelStep](), snapshotPath = None)
   def apply(project:String):Model = Model(project = project, model = project, modelGroup = "none", steps = Seq[ModelStep](), snapshotPath = None)
   def apply():Model = Model(project = "none", model = "none", modelGroup = "none", steps = Seq[ModelStep](), snapshotPath = None)
   def textClassifier(project:String, model:String):Model = Model(project = project, model = model, modelGroup = "Text Classification")
   def textClassifier(project:String):Model = Model(project = project, model = project, modelGroup = "Text Classification")

}


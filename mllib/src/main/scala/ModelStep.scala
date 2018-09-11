package demy.mllib

import demy.mllib.util.log
import org.apache.spark.ml.param.{ParamPair, ParamMap}
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types._
  
case class StepParam[+T](path:String, value:T, log:Boolean=false)
case class StepChoice(step:String, version:String) 
case class ModelStep(name:String, version:String, family:String, action:PipelineStage, log:Boolean=false, show:Boolean=false, pathsToLog:Seq[String]=Seq[String]()) {
    def applyParams(sParams:StepParam[Any]*) = {
        val appliedAction = sParams.foldLeft(this.action)((currentStage, sParam)=> ModelStep.applyParamToStage(currentStage, sParam))
        new ModelStep(name = this.name, version = this.version, family=this.family, action = appliedAction, log = log, show=show, pathsToLog=sParams.map(p => p.path).toSeq)
    }
    def structFieldAndValuesToLog() = {
        val paramPairs = pathsToLog.map(path => (ModelStep.getParam(action, path), path))
        paramPairs.map(t => 
        t match {
            case (ParamPair(_, v:Int), path) => (StructField(name = this.version+"_"+path, dataType=IntegerType), v)
            case (ParamPair(_, v:String), path) => (StructField(name = this.version+"_"+path, dataType=StringType), v)
            case (ParamPair(_, v:Float), path) => (StructField(name = this.version+"_"+path, dataType=FloatType), v)
            case (ParamPair(_, v:Double), path) => (StructField(name = this.version+"_"+path, dataType=DoubleType), v)
            case (ParamPair(_, v:Boolean), path) => (StructField(name = this.version+"_"+path, dataType=BooleanType), v)
            case (ParamPair(_, v:PipelineStage), path) => (StructField(name = this.version+"_"+path, dataType=StringType), v.getClass.getName.replace("org.apache.spark.ml.classification.", ""))
            case _ => throw new Exception("Unsuported parameter logging for this type, please extend (@epi)")
        })

    }
    def logStep() = ModelStep(name = name, version = version, family = family, action = action, log=true, show=show, pathsToLog=pathsToLog)
}

object ModelStep {
    def apply(name:String, action:PipelineStage):ModelStep = ModelStep(name = name, version= name , family = name, action = action)
    def apply(name:String, version:String, action:PipelineStage):ModelStep = ModelStep(name = name, version= version, family = name , action = action)
    def applyParamToStage(action:PipelineStage, param:StepParam[Any]):PipelineStage = {
        //log.msg(s"applyParamToStage: $action, ${param.path}, ${param.value}")
        val pathParts = param.path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot fin patrameter ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            val actionParam = action.getParam(pathParts(0))
            action.copy(new ParamMap()).set(actionParam, 
                                            ModelStep.applyParamToStage(
                                              action = action.get(actionParam).get.asInstanceOf[PipelineStage]
                                              , param = StepParam(path = pathParts.drop(1).mkString("."), value = param.value, log = param.log)
                                            ))
        } else {
            val theParam = action.getParam(pathParts(0))
            action.copy(new ParamMap()).set(theParam, param.value)
        }
    }

    def getParam(action:PipelineStage, path:String):ParamPair[Any] = {
        val pathParts = path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot find patrameter to log ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            val actionParam = action.getParam(pathParts(0))
            ModelStep.getParam(action = action.get(actionParam).get.asInstanceOf[PipelineStage], path = pathParts.drop(1).mkString("."))
        } else {
            val p = action.getParam(pathParts(0))
            ParamPair[Any](p, action.getOrDefault(p))
        }
    }
}

package demy.mllib

import demy.mllib.util.log
import org.apache.spark.ml.param.{ParamPair, ParamMap, Params}
import org.apache.spark.sql.types._
  
case class StepParam[+T](path:String, value:T, log:Boolean=false)
case class StepChoice(step:String, version:String) 
case class ModelStep(name:String, version:String, family:String, action:Params, log:Boolean=false, show:Boolean=false
                     , pathsToLog:Seq[String]=Seq[String](), cache:Boolean=false, input:Option[String]=None
                     , select:Seq[String]=Seq[String](), drop:Seq[String]=Seq[String](), paramInputs:Seq[(String, String)]=Seq[(String, String)]()
                     , snapshot:Boolean = false, reuseSnapshot:Boolean = false, renameCols:Seq[(String, String)]=Seq[(String, String)]()) {
    def applyParams(sParams:StepParam[Any]*) = {
        val appliedAction = sParams.foldLeft(this.action)((currentAction, sParam)=> ModelStep.applyParamToParams(currentAction, sParam))
        new ModelStep(name = this.name, version = this.version
                      , family=this.family, action = appliedAction
                      , log = log, show=show
                      , pathsToLog=this.pathsToLog ++ sParams.map(p => p.path).toSeq
                      , cache = this.cache
                      , input = this.input
                      , select = this.select
                      , drop = this.drop
                      , paramInputs = this.paramInputs
                      , snapshot = this.snapshot
                      , reuseSnapshot = this.reuseSnapshot
                      , renameCols = this.renameCols
                      )
    }
    def structFieldAndValuesToLog() = {
      val paramPairs = pathsToLog.map(path => (ModelStep.getParam(action, path), ModelStep.getLogNameDefault(action, path) match {case Some(s)=> s+"_"+path.split("\\.").last case _ => this.version+"_"+path}))
        paramPairs.map(t => 
        t match {
            case (ParamPair(_, v:Int), name) => (StructField(name = name, dataType=IntegerType), v)
            case (ParamPair(_, v:String), name) => (StructField(name = name, dataType=StringType), v)
            case (ParamPair(_, v:Float), name) => (StructField(name = name, dataType=FloatType), v)
            case (ParamPair(_, v:Double), name) => (StructField(name = name, dataType=DoubleType), v)
            case (ParamPair(_, v:Boolean), name) => (StructField(name = name, dataType=BooleanType), v)
            case (ParamPair(_, v:Array[Any]), name) => (StructField(name = name, dataType=StringType), v.mkString(","))
            case (ParamPair(_, v:Params), name) => (StructField(name = name, dataType=StringType), v.getClass.getName.split("\\.").last)
            case _ => throw new Exception("Unsuported parameter logging for this type, please extend (@epi)")
        })

    }
    def option(nameValues:(String, String)*) = 
      nameValues.foldLeft(this)((current, nameValue) => nameValue match { case (optName, value) => 
        ModelStep(name = current.name
          , version = current.version
          , family = current.family
          , action = current.action
          , log = if(optName == "log") value.toBoolean else current.log
          , show = if(optName == "show") value.toBoolean else current.show
          , pathsToLog = if(optName == "pathsToLog") value.split(',').toSeq else current.pathsToLog
          , cache = if(optName == "cache") value.toBoolean else current.cache
          , input = if(optName == "input") Some(value) else current.input
          , select = if(optName == "select") value.split(',').toSeq.map(s => s.trim) else current.select
          , drop = if(optName == "drop") value.split(',').toSeq.map(s => s.trim) else current.drop
          , paramInputs = if(optName == "paramInputs") value.split(',').toSeq.map(s => s.trim).map(s => (s.split("->")(0), s.split("->")(1))) else current.paramInputs
          , snapshot = if(optName == "snapshot") value.toBoolean else current.snapshot
          , reuseSnapshot = if(optName == "reuseSnapshot") value.toBoolean else current.reuseSnapshot
          , renameCols = if(optName == "renameCols") value.split(',').toSeq.map(s => s.trim).map(s => (s.split("->")(0), s.split("->")(1))) else current.renameCols
        )})
}

object ModelStep {
    def apply(name:String, action:Params):ModelStep = ModelStep(name = name, version= name , family = name, action = action)
    def apply(name:String, version:String, action:Params):ModelStep = ModelStep(name = name, version= version, family = name , action = action)
    def apply(name:String, version:String, action:Params, show:Boolean):ModelStep = ModelStep(name = name, version= version, family = name , action = action, show = show)
    def applyParamToParams(action:Params, param:StepParam[Any]):Params = {
        //log.msg(s"Action: $action, ${param.path}, ${param.value}")
        val pathParts = param.path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot find patrameter ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            val actionParam = action.getParam(pathParts(0))
            action.copy(new ParamMap()).set(actionParam, 
                                            ModelStep.applyParamToParams(
                                              action = action.get(actionParam).get.asInstanceOf[Params]
                                              , param = StepParam(path = pathParts.drop(1).mkString("."), value = param.value, log = param.log)
                                            ))
        } else {
            val actionCopy = action.copy(new ParamMap())
            val theParam = actionCopy.getParam(pathParts(0))
            actionCopy.set(theParam, param.value)
        }
    }

    def getParam(action:Params, path:String):ParamPair[Any] = {
        val pathParts = path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot find patrameter to log ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            val actionParam = action.getParam(pathParts(0))
            ModelStep.getParam(action = action.get(actionParam).get.asInstanceOf[Params], path = pathParts.drop(1).mkString("."))
        } else {
            val p = action.getParam(pathParts(0))
            ParamPair[Any](p, action.getOrDefault(p))
        }
    }

    def getParamParent(action:Params, path:String):Params = {
        val pathParts = path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot find patrameter to log ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            val actionParam = action.getParam(pathParts(0))
            ModelStep.getParamParent(action = action.get(actionParam).get.asInstanceOf[Params], path = pathParts.drop(1).mkString("."))
        } else {
          action
        }
    }
    def getLogNameDefault(action:Params, path:String) ={
        val pathParts = path.split("\\.")
        if(!action.hasParam(pathParts(0)))
            throw new Exception(s"Cannot find patrameter to log ${pathParts(0)} on stage ${action.getClass.getName}")
        if(pathParts.size > 1) {
            //Nested pipeline stage parameter
            Some(ModelStep.getParamParent(action = action, path = path).getClass.getName.split("\\.").last)
        } else {
          None
        }
    }
}

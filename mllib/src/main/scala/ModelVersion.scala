package demy.mllib

import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema

case class ModelVersion(steps:Seq[ModelStep]=Seq[ModelStep](), comment:String="", customLogs:GenericRowWithSchema=new GenericRowWithSchema(values= Array[Any](), schema=new StructType(Array[StructField]()))) {
    def isReady() {
      val names = this.steps.map(step => step.name)
      names.size == names.distinct.size
    }
    def printSchema() {
        val msg = new scala.collection.mutable.ArrayBuffer[String]()
        customLogs.schema.fields.zip(customLogs.toSeq)
           .foreach(p => p match {case (field, value) => msg+= s"${field.name}=>${value}"})
        msg += this.comment
        this.steps.foreach{step => 
            msg += step.toString
        }
        println(msg.mkString("\n"))
    }
    def step(stepChoices:(String, String)*) = {
        val choices = stepChoices.map(c => StepChoice(c._1, c._2))
        val repeatedChoices = choices.groupBy(c =>c.step).flatMap(p => p match {case (step, choices) => if(choices.size > 1) Some((step, choices.map(c => c.version))) else None})
        if(repeatedChoices.size > 0) throw new Exception(s"cannot choose more than one version (${repeatedChoices.map(rc => rc match {case (step, versions) => s"${step}[${versions.mkString(";")}]"}).mkString("|") })")
        val noMatchs = choices.filter(c => this.steps.filter(s => s.name == c.step && s.version == c.version).size != 1).map(c => c.step)
        if(noMatchs.size > 0) throw new Exception(s"Step(s) ${noMatchs.mkString(", ")} should exists exactly once among (${this.steps.map(step =>  (step.name, step.version)).mkString})")
        
        val newSteps = this.steps
                            .filter(step => choices.filter(c => c.step == step.name).size == 0 || choices.filter(c => c.step == step.name && c.version == step.version ).size == 1)
                            .map(step => if(choices.filter(c => c.step == step.name && c.version == step.version ).size == 1) step.logStep else step)
        ModelVersion(steps = newSteps, comment = this.comment, customLogs = this.customLogs)
    }
    def drop(stepsToDrop:String*) = {
        val allSteps = this.steps.map(step => step.name).toSet
        val dropSet = stepsToDrop.toSet
        val nonExisting = dropSet.diff(allSteps)
        if(nonExisting.size > 0) throw new Exception(s"Cannot fin the following(s) step to drop: ${nonExisting.mkString(",")} between ${allSteps.mkString(",")}")

        val newSteps = this.steps.filterNot(step => dropSet.contains(step.name))
        
        ModelVersion(steps = newSteps, comment = this.comment, customLogs = this.customLogs)
    }
    def comment(commentToAdd:String):ModelVersion =  {
        val newComment = if(this.comment == null || this.comment.size == 0) commentToAdd else this.comment + ", " + commentToAdd
        ModelVersion(steps = this.steps, comment = newComment, customLogs = this.customLogs)
    }
    def params(step:String, paramPairs:(String, Any)*):ModelVersion = {
        val versionNames = this.steps.flatMap(s => if(s.name == step) Some(s.version) else None)
        if(versionNames.size == 0) throw new Exception(s"Cannot find step ${step}: between [${this.steps.map(step => step.name).mkString(", ")}]")
        versionNames.foldLeft(this)((current, versionName)=>current.params(step, versionName, paramPairs :_*))
    }
    def params(step:String, version:String, paramPairs:(String, Any)*):ModelVersion = {
        val stepParams = paramPairs.map(p => StepParam[Any](path = p._1, value=p._2, log = true))
        val selectedSteps = this.steps.filter(s => s.name == step && s.version == version)        
        if(selectedSteps.size != 1) throw new Exception(s"parameter ($step,$version) should match a single stem instead of ${selectedSteps.size}")
        val theSteps = this.steps.map(s => if(s.name == step && s.version == version) s.applyParams(stepParams:_*).logStep else s)
        ModelVersion(steps = theSteps, comment = this.comment, customLogs = this.customLogs)
    }
    def log(toLog:(String,Any)*) = {
      var fields:Seq[StructField] = customLogs.schema.fields
      fields = fields ++ toLog.map(p => p match {case (name, value) => 
            new StructField(name = name, dataType = (
              value match {
                case (v:Int) => IntegerType
                case (v:String) => StringType
                case (v:Float) => FloatType
                case (v:Double) => DoubleType
                case (v:Boolean) => BooleanType
                case _ => StringType
              }))
          }).toSeq 
      var values:Seq[Any] = customLogs.toSeq
      values = values ++ toLog.map(p => p match{case (name, value) => 
              value match {
                case (v:Int) => v
                case (v:String) => v
                case (v:Float) => v
                case (v:Double) => v
                case (v:Boolean) => v
                case v => v.toString
              }}).toSeq 
        ModelVersion(steps = this.steps, comment = this.comment, customLogs = new GenericRowWithSchema(schema =  new StructType(fields.toArray),values = values.toArray))
    } 
}

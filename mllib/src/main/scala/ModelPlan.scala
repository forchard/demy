package demy.mllib

import demy.mllib.util.log

case class ModelPlan(transformations:Seq[(ModelVersion)=>Seq[ModelVersion]] = Seq[(ModelVersion)=>Seq[ModelVersion]]()) {
    def set(transformations:((ModelVersion)=>ModelVersion)*) 
           = ModelPlan(transformations = this.transformations ++ (transformations.toSeq.map(t => ((version:ModelVersion)=> Seq(t(version))))
             ))
    
    def switch(transformations:((ModelVersion)=>Any)*) 
           = ModelPlan(transformations = this.transformations :+ ((version:ModelVersion)=> transformations.flatMap(t => 
                  (t(version) match {
                    case res:Seq[_] => res 
                    case res => Seq(res)
                  }).map(v => v match {
                    case m:ModelVersion => m
                    case m => throw new Exception("Switch does not support ${m.getType.getName} as Model Version @epi")
                  })
             )))
    def merge(these:ModelPlan*)  
           = ModelPlan(transformations = Seq(((v:ModelVersion)=>(
               ModelPlan.applyTransformations(currents = Seq(v), transformations = this.transformations)
               ++
               these.flatMap(that => ModelPlan.applyTransformations(currents = Seq(v), transformations = that.transformations))
               )))
             )
    def build(base:ModelVersion, stopAfter:Option[String]=None) = {
      ModelPlan.applyTransformations(Seq(stopAfter match {case Some(step) => base.dropAfter(step) case _ => base}) , this.transformations)
    }
    def repeat(n:Int) = Seq.fill(n-1)(this).foldLeft(this)((current, iter)=>current.merge(iter))
    def log(property:String, value:String)  
      = ModelPlan(transformations = this.transformations :+ ((model:ModelVersion) => Seq(model.log(property->value))))
}

object ModelPlan {
    def applyTransformations(currents:Seq[ModelVersion], transformations:Seq[(ModelVersion)=>Seq[ModelVersion]]):Seq[ModelVersion] = {
        val ret = 
          if(transformations.size == 0)
            currents
          else
            ModelPlan.applyTransformations(currents = currents.flatMap(version => transformations(0)(version))
                                           , transformations = transformations.drop(1))
        ret
    }
}


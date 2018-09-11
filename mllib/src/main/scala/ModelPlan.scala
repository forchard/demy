package demy.mllib

import demy.mllib.util.log

case class ModelPlan(base:ModelVersion, transformations:Seq[(ModelVersion)=>Seq[ModelVersion]] = Seq[(ModelVersion)=>Seq[ModelVersion]]()) {
    def set(transformations:((ModelVersion)=>ModelVersion)*) 
           = ModelPlan(base = this.base
                       , transformations = this.transformations ++ (transformations.toSeq.map(t => ((version:ModelVersion)=> Seq(t(version))))
             ))
    def switch(transformations:((ModelVersion)=>Seq[ModelVersion])) 
           = ModelPlan(base = this.base
                       , transformations = this.transformations :+ transformations)
    def switch(transformations:((ModelVersion)=>ModelVersion)*) 
           = ModelPlan(base = this.base
                       , transformations = this.transformations :+ ((version:ModelVersion)=> transformations.toSeq.map(t => t(version))
             ))
    def build() = {
      ModelPlan.applyTransformations(Seq(this.base), this.transformations)
    }
}

object ModelPlan {
    def applyTransformations(currents:Seq[ModelVersion], transformations:Seq[(ModelVersion)=>Seq[ModelVersion]]):Seq[ModelVersion] = {
        val ret = 
          if(transformations.size == 0)
            currents
          else
            ModelPlan.applyTransformations(currents = currents.flatMap(version => transformations(0)(version))
                                           , transformations = transformations.drop(1))
        log.msg(s"${ret.size} versions produced so far")
        ret
    }
}


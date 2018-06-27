package demy.mllib.search

import scala.collection.mutable.ArrayBuffer
import scala.collection.Map

case class PTree[T](var options:Seq[PTree[T]], value:T, p:Double, level:Int) {
  def step(sequence:Seq[Seq[T]],probMap:Map[(T, T, String), Double], probTypes:Seq[String]) = {
    val candidates = sequence(this.level+1)
    this.options = candidates.map(c => PTree[T](options=Seq[PTree[T]]()
                                                , value = c
						, p = probTypes.map(pType => probMap.getOrElse((this.value, c, pType), 0.0)).reduce(_ * _)
                                                , level = level + 1
                    ))
    this
  }

  def getP(currentLevel:Int):Double = if(this.level == currentLevel) this.p else this.p * this.options.map(c => c.getP(currentLevel)).max
  def chooseBestChild(currentLevel:Int) = {
    this.options = Seq(this.options.map(c => (c, c.getP(currentLevel))).reduce((scored1,scored2) => 
                                                                      (scored1,scored2) match {case ((c1, p1),(c2, p2))
                                                                                        => if(p1 > p2) scored1 else scored2 })._1)
  }
  def getDescendants(currentLevel:Int):Seq[PTree[T]] = {
    if(this.level == currentLevel) Seq(this)
    else this.options.flatMap(c => c.getDescendants(currentLevel))
  }
} 
object PTree {
  def evaluate[T](sequence:Seq[Seq[T]], maxLeafs:Int = 32, probMap:scala.collection.Map[(T, T, String), Double], probTypes:Seq[String], default:T) = {
    var iRes = 0
    var iFetch = 0 
    val ret = ArrayBuffer[T]()
    var leafs = sequence(0).map(v => PTree[T](options = Seq[PTree[T]](), value = v, p=1.0, level = 0))
    var toChoose = PTree[T](options = leafs, value = null.asInstanceOf[T], p = 1.0, level = -1)
    val sequenceSize = sequence.size

    println(s"root, ${leafs.size} nodes")
    while(iRes < sequenceSize) {
      if(toChoose.options.size == 0) {
        println("we found an empty option, applying default")
        toChoose.options = Seq(PTree[T](options = Seq[PTree[T]](), value = default, p = 0.0, level = iRes))
        leafs = toChoose.options
      }
      var levelCount = leafs.size
      while(levelCount > 0 && levelCount < maxLeafs && iFetch < sequenceSize -1) {
        leafs.foreach(l => l.step(sequence, probMap, probTypes))
        leafs = leafs.flatMap(l => l.options)
        levelCount = leafs.size
        iFetch = iFetch + 1        
      }
      println(s"level $iRes, $levelCount nodes")
      if(levelCount == 0) {
        println(s"Expansion $iFetch lead to no possible solution choosing a level before expansion")
        toChoose.chooseBestChild(iFetch - 1)
      } else { 
        println(s"Expansion $iFetch lead to max leafs or array end using current information ")
        toChoose.chooseBestChild(iFetch)
      }
      val chosen = toChoose.options(0)
      println(s"level $iRes, $chosen has been chosen")
      ret.append(chosen.value)
      toChoose = chosen
      leafs = chosen.getDescendants(iFetch)
      iRes = iRes + 1
    }
    ret
  } 
}


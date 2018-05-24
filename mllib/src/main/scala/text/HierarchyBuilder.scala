package demy.mllib.text

//import demy.mllib.linalg.SemanticVector


case class HierarchyBuilder(leafs:Array[CenterTagged]) {    
    val nodes = scala.collection.mutable.Map(leafs.map(c => (c.centerId, c)):_*)
    val parentOf = scala.collection.mutable.Map[Int, Int]()
    val childOf = scala.collection.mutable.Map[Int, Seq[Int]]()

    def addParentsFor(focus:Array[Int], theNextId:Int):Int = {
        //println(s"Adding parents for (${focus.mkString(",")}")
        var nextId = theNextId
        Range(0, focus.size).foreach(leftInd => {
            if(!parentOf.contains(focus(leftInd))) {
                var bestRightInd:Option[Int] = None 
                var bestDistance = Double.MaxValue
                Range(leftInd+1, focus.size).foreach(rightInd => {
                    if(!parentOf.contains(focus(rightInd))) {
                        var distance = nodes(focus(leftInd)).center.distanceWith(nodes(focus(rightInd)).center)
                        if(distance < bestDistance) {
                            bestDistance = distance
                            bestRightInd = Some(rightInd)
                        }
                    }
                })
                var children = Array(Some(nodes(focus(leftInd))), bestRightInd match {case Some(ind) => Some(nodes(focus(ind))) case _ => None} ).flatMap(d => d)
                var parent = CenterTagged(centerId = nextId, center = children.map(c => c.center).reduce((v1, v2)=>v1.sum(v2)), tags = children.map(c => c.tags.toSet).reduce((t1, t2)=> t1.intersect(t2)).toSeq)
                children.foreach(node => {
                    parentOf(node.centerId) = nextId
                })
                childOf(nextId) = children.map(c => c.centerId)
                nodes(nextId) = parent
    //            println(s"$parent <-- (${children.mkString(",")})")
                nextId = nextId + 1
            }        
        })
        nextId
    }
    def mergeNodes(focus:Array[(Int, Array[Int])], maxMerges:Int) {
        //maxMerges defines the number of parents to be merged with other parents
        //foreach parent on focus we are going to find the closest parent siblings on quequence (on focus) to apply the merge 
        //println(s"Going to merge $maxMerges")
    //    focus.foreach(p => println(s"${p._1} with ob of (2.mkString(",")})}"))
        val alreadyMerged = scala.collection.mutable.Set[Int]()
        var nodesMerged = 0
        var index = 0
        while(nodesMerged < maxMerges && index < focus.size) {
            val toMerge = focus(index)._1
            if(!alreadyMerged.contains(toMerge)) {
                alreadyMerged.add(toMerge)
                var bestParent:Option[Int] = None
                var bestDistance = Double.MaxValue
                focus(index)._2.foreach(parentTarget => {
                    if(!alreadyMerged.contains(parentTarget)) {
                        val distance = nodes(toMerge).center.distanceWith(nodes(parentTarget).center)
                        if(distance < bestDistance) {
                            bestDistance = distance
                            bestParent = Some(parentTarget)
                        }
                    }
                })
                bestParent match {
                    case Some(choosenNode) => {
                        alreadyMerged.add(choosenNode)
                        val children = childOf(choosenNode) ++ childOf(toMerge)
                        nodes(choosenNode) = CenterTagged(centerId = choosenNode, center = children.map(c => nodes(c).center).reduce((v1, v2) => v1.sum(v2)), tags = nodes(choosenNode).tags.toSet.intersect(nodes(toMerge).tags.toSet).toSeq)
                        childOf(toMerge).foreach(c => parentOf(c) = choosenNode)
                        childOf.remove(toMerge)
                        childOf(choosenNode) = children
                        nodes.remove(toMerge)
                        nodesMerged = nodesMerged + 1
                    }
                    case _ => {}
                }
            }
            index = index + 1
        }
    //    childOf.toSeq.sortWith(_._1<_._1).map(p => p match { case (parent, children) =>  println(s"$parent ==> ${children.mkString(",")}")})
    }
    def splitNodes(focus:Array[Int], maxSplits:Int, theNextId:Int) = {
        println(s"Going to split $maxSplits")
    //    println(s"to split: ${focus.mkString(",")}")
        var nextId = theNextId
        //we are going to iterate through focus nodes and splitting parents onto single parents until the max split is reached
        var nodesSplit = 0
        var index = 0
        
        while(nodesSplit < maxSplits && index < focus.size) {
            val nodeToSplit = focus(index)
            if(childOf(nodeToSplit).size > 1) {
                val singleChild =  nodes(childOf(nodeToSplit)(0))
                val rest =  childOf(nodeToSplit).slice(1, childOf(nodeToSplit).size).map(c => nodes(c))
                val split1 = CenterTagged(centerId = nextId, center = singleChild.center, tags = singleChild.tags)
                val split2 = CenterTagged(centerId = nodeToSplit, center = rest.map(c => c.center).reduce((v1, v2)=> v1.sum(v2)), tags = rest.map(c => c.tags.toSet).reduce((t1, t2) => t1.intersect(t2)).toSeq)
                nodes(nextId) = split1
                nodes(nodeToSplit) = split2
                parentOf(singleChild.centerId) = nextId
                childOf(nextId) = Array(singleChild.centerId)
                childOf(nodeToSplit) = rest.map(c => c.centerId)
                nodesSplit = nodesSplit + 1
                nextId = nextId + 1
            }
            if(childOf(nodeToSplit).size == 1)
                index = index + 1
        }
    //    childOf.toSeq.sortWith(_._1<_._1).map(p => p match { case (parent, children) =>  println(s"$parent ==> ${children.mkString(",")}")})
        nextId
    }
    
    def createParents(focus:Array[Int]) = {
        //We will ensure to produce a sound tree which is
        //all non leaf levels has an power of 2 number of nodes
        //each tag will have a single node being ancestor of all nodes with that tag (=> #tags by cluster <= 1) 
        //parent calculation is done on two steps: 
        //Balanced affectation:
        // - foreach tag with more than 1 cluster generate a tagged slot for by pairs and a single tagged slot when odd number of nodes 
        // - remaining centers will generate as pairs until the last is reached that can be   
        //Adjustment to target #of parents (target number of parents is the closest power of 2 below the number of centers)
        // - calculate the gap with against the target parent number it can be positive or negative
        // - while current parent count is bigger than target merge parents (single clusters in tag with n>3 in priority, then single cluster on non tagged clusters then pairs of non tagged clusters then pairs of tagged)
        // - while current parent count is lesser than target split parent pairs of tags non tagged as priority and tagged pairs later
        val centers = focus.map(id => nodes(id))
        val centersByTag = centers.groupBy(center => center.tags)
        var nextId = centers.map(center => center.centerId).max + 1
    
        //Balanced affectation:
        //Balanced affectation: foreach tag with more than 1 cluster generate a tagged slot for pairs and a single tagged slot when odd number of nodes 
        centersByTag
          .foreach(p => {
            p match { case (tags, centersInTag) => {
              if(tags.size > 0 && centersInTag.size > 1)
                nextId = addParentsFor(focus = centersInTag.map(c => c.centerId), theNextId = nextId)
            }}
        })
        //Balanced affectation: remaining centers will generate as pairs until the last is reached that can be
        val remainingCenters = centers.filter(node => node.tags.size == 0 || centersByTag(node.tags).size == 1)
        nextId = addParentsFor(focus = remainingCenters.map(c => c.centerId), theNextId = nextId)
    
        //childOf.toSeq.sortWith(_._1<_._1).map(p => p match { case (parent, children) =>  println(s"$parent ==> ${children.mkString(",")}")})
    
        //Adjustment to target #of parents 
        val parentsTarget = Math.pow(2, Math.ceil(Math.log(centers.size)/Math.log(2)) - 1).round.toInt
        var clustersToAdd = parentsTarget - centers.flatMap(center => parentOf.get(center.centerId)).distinct.size
        while(clustersToAdd != 0) {
            println(s"target = $parentsTarget we have ${centers.flatMap(center => parentOf.get(center.centerId)).distinct.size} we need to add $clustersToAdd")
            if(clustersToAdd < 0) {
                //println("Going to reduce parents")
                val parentCandidates = centersByTag.map(p => p match {case (tags, centers) => (tags, centers.map(c => parentOf(c.centerId)).distinct)}).toMap
                // merge parents (single clusters in tag with odd n>2 in priority, then single cluster on non tagged clusters then pairs of non tagged clusters then pairs of tagged clusters )
                val p1toMerge = centers.filter(center => center.tags.size > 0 && centersByTag(center.tags).size > 2 && parentOf.contains(center.centerId) && childOf(parentOf(center.centerId)).size == 1)
                                       .map(center => parentOf(center.centerId))
                if(p1toMerge.size > 0) {
                  //println("P1")
                  val target = p1toMerge.map(id => (id, parentCandidates(nodes(id).tags))) 
                  mergeNodes(focus = target, maxMerges = -clustersToAdd)
                } else { //single cluster on non tagged clusters
                  val p2toMerge = centers.filter(center => center.tags.size == 0 && centersByTag(center.tags).size > 2 && parentOf.contains(center.centerId) && childOf(parentOf(center.centerId)).size == 1)
                                         .map(center => parentOf(center.centerId))
                  if(p2toMerge.size > 0) {
                  //println("P2")
                  val parentCandidates = centersByTag.map(p => p match {case (tags, centers) => (tags, centers.map(c => parentOf(c.centerId)).distinct)}).toMap
                  val target = p2toMerge.map(id => (id, parentCandidates(nodes(id).tags))) 
                  mergeNodes(focus = target, maxMerges = -clustersToAdd)
                  }
                  else { //pairs of non tagged clusters
                    val p3toMerge = centers.filter(center => center.tags.size == 0 && centersByTag(center.tags).size > 2 && parentOf.contains(center.centerId) && childOf(parentOf(center.centerId)).size == 2)
                                         .map(center => parentOf(center.centerId)).distinct
                    if(p3toMerge.size > 0) {
                    //println("P3")
                      val target = p3toMerge.map(id => (id, parentCandidates(nodes(id).tags))) 
                      mergeNodes(focus = target, maxMerges = -clustersToAdd)
                    } else { //pairs of tagged clusters
                      val p4toMerge = centers.filter(center => center.tags.size > 0 && centersByTag(center.tags).size > 2 && parentOf.contains(center.centerId) && childOf(parentOf(center.centerId)).size == 2)
                                         .map(center => parentOf(center.centerId)).distinct
                      if(p4toMerge.size > 0) {
                          //println("P4")
                          val target = p4toMerge.map(id => (id, parentCandidates(nodes(id).tags))) 
                          mergeNodes(focus = target, maxMerges = -clustersToAdd)
                      } else {
                        throw new Exception("I do not know how to keep reducing the number of parents")
                      }
                    }
                  }
                }
            }
            else //(clustersToAdd > 0): split pairs of tags non tagged as priority and tagged pairs later
            {
                //println("Going to add parents")
                val p1toSplit = centers.map(center => parentOf(center.centerId)).filter(parent => childOf(parent).size > 1).distinct.filter(p => nodes(p).tags.size == 0).sortWith((p1, p2) => childOf(p1).size > childOf(p2).size)
                if(p1toSplit.size > 0) {
                  //println("P1")
                  nextId = splitNodes(focus = p1toSplit, maxSplits = clustersToAdd, theNextId = nextId)
                } else { //tagged pairs 
                  val p2toSplit = centers.map(center => parentOf(center.centerId)).filter(parent => childOf(parent).size > 1).distinct.filter(p => nodes(p).tags.size > 0).sortWith((p1, p2) => childOf(p1).size > childOf(p2).size)
                  if(p2toSplit.size > 0) {
                    //println("P2")
                    nextId = splitNodes(focus = p2toSplit, maxSplits = clustersToAdd, theNextId = nextId)
                  }
                  else { //pairs of non tagged clusters
                    throw new Exception("I do not know how to keep splitting the number of parents")
             }
                }
            }
            clustersToAdd = parentsTarget - centers.flatMap(center => parentOf.get(center.centerId)).distinct.size
        }
        focus.map(node => parentOf(node)).distinct
    }
    def getHierarchy(nodeId:Int) = {
        val ret = scala.collection.mutable.ArrayBuffer(nodeId)
        var parent = this.parentOf.get(nodeId)
        while(!parent.isEmpty) {
            val parentId = parent.get
            ret.append(parentId)
            parent = this.parentOf.get(parentId)
        }
        ret.reverse.toSeq
    }
    def buildHierarchy() = {
        var levelNodes = leafs.map(n => n.centerId)
        var i = 0
        while(levelNodes.size > 1) {
            println(s"Grouping level $i with ${levelNodes.size} nodes")
            levelNodes = createParents(levelNodes)
            //levelNodes.map(n => (n, childOf(n))).toSeq.sortWith(_._1<_._1).map(p => p match { case (parent, children) =>  println(s"$parent (${nodes(parent).tags}) ==> ${children.mkString(",")}")})
            i = i + 1
        }
        nodes.values.map(node => node.toNodeTagged(this.getHierarchy(node.centerId)))
    }


}

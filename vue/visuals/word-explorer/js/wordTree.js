Array.prototype.flatMap = function(lambda) { 
  return Array.prototype.concat.apply([], this.map(lambda)); 
};
Array.prototype.distinct = function() { 
  return Array.from(new Set(this))
};

function WordTree() {
  this.root = null;
  this.fromPath = function(jsonPath, callback) {
    var thisWt = this
    d3.json(jsonPath, function(error, root) {
      if(error){
        alert(error)
        console.log(error)
      } else { 
	thisWt.root = root;
        if(callback) callback(thisWt);
      }
    });
    return this;
  };
  this.sameHierarchy = function(h1, h2) {
    return h1.length===h2.length && h1.every((v,i) => v === h2[i])
  };
  this.getRatioRange = function(node) {
    return node.children.map(c =>this.getRatioRange(c))
                    .concat({"min":node.density, "max":node.density})
                    .reduce((p1, p2)=> r = {"min":p1.min<p2.min?p1.min:p2.min, "max":p1.max>p2.max?p1.max:p2.max});
  };

  this.toLeafOnlyHierarchy = function(node, level = 10) {
    var ret = {}; 
    ret.name=node.name;
    ret.size=node.size;
    ret.docCount=node.size;
    ret.id=node.hierarchy.join(",");
    ret.childrenHidden=node.childrenHidden;
    ret.leaf=node.leaf;
    ret.isWord=false;

    var index = node.words.length;
    var length = node.words.length; 
    if(node.children == 0)
    	ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "noCount":(index<length)
                                                , "size":(node.size * ((index--)+length*0.2)/(1.2*length))|0
                                                , "id":ret.id+":"+w, "isWord":true}
                                     )
                                                                 
    else
        ret.children = [];

    ret.children = ret.children.concat(node.children.map(c => this.toLeafOnlyHierarchy(c, level - 1)))
    return ret;
  };
  
  this.toFullHierarchy = function(node) {
    var ret = {}; 
    ret.name=node.name;
    ret.size=node.size;

    ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "size":node.size })

    ret.children = ret.children.concat(node.children.map(c => this.toFullHierarchy(c)))
    return ret;
  };
  this.slice = function(node, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes, selectedNodes, taggedNodes, tags, includeNoTag, parentVisible) {
    var nodeId = node.hierarchy.join(",");
    var parentId = node.hierarchy.slice(0, node.hierarchy.length-1).join(",");
    var isSelected = selectedNodes.includes(nodeId)
    var isCollapsed = collapsedNodes.includes(nodeId)
    var isExpanded = expandedNodes.includes(nodeId)
    var isParentExpanded = expandedNodes.includes(parentId) && parentVisible
    var isTagged = Boolean(taggedNodes[nodeId]) && taggedNodes[nodeId].length > 0
    var removedByTag =
          (!isTagged && !includeNoTag)
          | ( isTagged && Object.keys(tags).filter(t => tags[t].withinSearch && taggedNodes[nodeId].includes(t)).length == 0 )
    var matchingTag = isTagged && Object.keys(tags).filter(t => tags[t].withinSearch && taggedNodes[nodeId].includes(t)).length > 0 
    var ignoreLevel = (!isParentExpanded) && 
                      ( node.hierarchy.length<startLevel 
                         || node.size > maxSize 
                         || node.density < minRatio || node.density > maxRatio 
                         || Boolean(wordFilter) && node.words.filter(p => p.match(wordFilter)).length == 0
                         || removedByTag
                      )
    var canSplit = node.children.length>1 
                   && !isCollapsed
                   && ((node.children.filter(c => c.size<minSize).length == 0
                       && node.hierarchy.length<endLevel
                       && !(matchingTag) 
                      ) || isExpanded)
    //Looking for focus node
    if(node.hierarchy.length < hierarchy.length) {
      var child = node.children.filter(c => c.hierarchy.slice(0, node.hierarchy.length+1).join(",") === hierarchy.slice(0, node.hierarchy.length+1).join(","))[0]
      return this.slice(child, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes, selectedNodes, taggedNodes, tags, includeNoTag, false);
    }
    //focus node found
    else if(node.hierarchy.length==hierarchy.length && node.hierarchy.length<startLevel)
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":[]
              , "children":node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes, selectedNodes, taggedNodes, tags, includeNoTag, true))
              , "childrenHidden":false, "leaf":node.children.length==0, "selected":isSelected}
    //Ignoring nodes out of limits but giving a chance to children 
    else if(ignoreLevel && canSplit) {
      return node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes, selectedNodes, taggedNodes, tags, includeNoTag, false))
    }
    else if(!ignoreLevel)
    {
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":node.words
              , "children": canSplit?node.children.flatMap(c => [].concat(this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes, selectedNodes, taggedNodes, tags, includeNoTag, true))):[]
              , "childrenHidden":(node.children.length>0 && !canSplit), "leaf":node.children.length==0, "selected":isSelected}
    }
    return [];
  };

}

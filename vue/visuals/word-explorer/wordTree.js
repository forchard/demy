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
      if(error)
        alert(error)
      else { 
        thisWt.root = root
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
    ret.phrases=node.phrases;
    ret.id=node.hierarchy.join(",");
    ret.childrenHidden=node.childrenHidden;

    var index = node.words.length;
    var length = node.words.length; 
    if(node.children == 0)
    	ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "noCount":(index<length)
                                                , "size":(node.size * ((index--)+length*0.2)/(1.2*length))|0
                                                , "phrases":node.phrases.filter(p => p.includes(w))
                                                , "id":ret.id+":"+w, "drillDown":false}
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

    ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "size":node.size, "phrases":node.phrases })

    ret.children = ret.children.concat(node.children.map(c => this.toFullHierarchy(c)))
    return ret;
  };
  this.slice = function(node, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes) {
    var isCollapsed = collapsedNodes.includes(node.hierarchy.join(","))
    var isExpanded = expandedNodes.includes(node.hierarchy.join(","))
    var isParentExpanded = expandedNodes.includes(node.hierarchy.slice(0, hierarchy.length-1).join(","))
    var ignoreLevel = (!isParentExpanded) && 
                      ( node.hierarchy.length<startLevel 
                         || node.size > maxSize 
                         || node.density < minRatio || node.density > maxRatio 
                         || Boolean(wordFilter) && node.words.filter(p => p.match(wordFilter)).length == 0
                      )
    var canSplit = node.children.length>1 
                   && !isCollapsed
                   && ((node.children.filter(c => c.size<minSize).length == 0
                       && node.hierarchy.length<endLevel
                      ) || isExpanded)
    //Looking for focus node
    if(node.hierarchy.length < hierarchy.length) {
      var child = node.children.filter(c => c.hierarchy.slice(0, node.hierarchy.length+1).join(",") === hierarchy.slice(0, node.hierarchy.length+1).join(","))[0]
      return this.slice(child, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes);
    }
    //focus node found
    else if(node.hierarchy.length==hierarchy.length && node.hierarchy.length<startLevel)
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":[]
              , "children":node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes))
              , "phrases":node.phrases}
    //Ignoring nodes out of limits higher than limits but giving a chance to children 
    else if(ignoreLevel && canSplit)
      return node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes))
    else if(!ignoreLevel)
    {
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":node.words
              , "children": canSplit?node.children.flatMap(c => [].concat(this.slice(c, hierarchy, startLevel, endLevel, minSize, maxSize, minRatio, maxRatio, wordFilter, expandedNodes, collapsedNodes))):[]
              , "phrases":node.phrases, "childrenHidden":(node.children.length>0 && !canSplit)}
    }
    return [];
  }

}

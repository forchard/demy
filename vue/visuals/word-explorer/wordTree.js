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
  this.removeNotFrequent = function(node, ancestor, goDown) {
    if(!this.sameHierarchy(ancestor.hierarchy, node.hierarchy)) {
      var tWords = node.words
      tWords.forEach(tw => {
      //If the current word exists on ancestor but frequency is less than 1.5 times or containing less than the half of occurrences than the ancestor 
      //the word will be removed from the children
      if(ancestor.words.filter(aw => aw.hash===tw.hash && (1.0*(tw.count/node.totalWords) / (aw.count/ancestor.totalWords)<=1 || tw.count <= aw.count/2.0)).length>0) 
        node.words = node.words.filter(w => tw.hash!=w.hash)                
      });
      this.removeNotFrequent(node, ancestor.children.filter(c => this.sameHierarchy(node.hierarchy.slice(0, c.hierarchy.length), c.hierarchy))[0], false)
    }
    if(goDown) 
      node.children.forEach(c => this.removeNotFrequent(c, ancestor, true));
  };
  this.removeNotFrequentOnAncestor = function(node, ancestor, goDown) {
    if(!this.sameHierarchy(ancestor.hierarchy, node.hierarchy)) {
      var tWords = node.words
      tWords.forEach(tw => {
      //If the current word exists on ancestor but frequency is less than 1.5 times or containing less than the half of occurrences than the ancestor 
      //the word will be removed from the children
      if(ancestor.words.filter(aw => aw.hash===tw.hash && (1.0*(tw.count/node.totalWords) / (aw.count/ancestor.totalWords)>1 || tw.count > aw.count/2.0)).length>0) 
        ancestor.words = ancestor.words.filter(w => tw.hash!=w.hash)                
      });
      this.removeNotFrequentOnAncestor(node, ancestor.children.filter(c => this.sameHierarchy(node.hierarchy.slice(0, c.hierarchy.length), c.hierarchy))[0], false)
    }
    if(goDown) 
      node.children.forEach(c => this.removeNotFrequentOnAncestor(c, ancestor, true));
  };
  this.removeEmpty = function(node) {
    while(node.children.length>0 && node.children.filter(c => c.words.length===0 ).length>0) {
        var toRemove = node.children.filter(c => c.words.length==0);
        var toAdd = toRemove.flatMap(c => c.children).filter(gc => gc.words.length>0 || gc.children.length>0);
        node.children = (node.children.filter(c => !toRemove.map(r => r.hierarchy.join(",")).includes(c.hierarchy.join(","))).concat(toAdd));
    }
    node.children.forEach(c => this.removeEmpty(c));
  };
  this.removeLeafsBiggerThan = function(node, limit) {
    node.words = node.words.slice(0,limit)
    node.children.forEach(c => this.removeLeafsBiggerThan(c, limit))
  };
  this.getHeadWord = function(node) {
    var h = node.words.concat(node.children.map(c =>this.getHeadWord(c))).sort((w1, w2) => w1.count < w2.count);
    if(h.length>0)
      return h[0]
    return null;
  };
  this.popUpSingleWordLeaf = function(node) {
    node.children = node.children.filter(c => c.children.length > 0 || c.words.length>1);
    node.words = node.words.concat(node.children.filter(c => c.children.length == 0 && c.words.length==1).flatMap(c => c.words)).sort((w1, w2) => w1.count < w2.count);
    node.children.forEach(c => this.popUpSingleWordLeaf(c));
  };
  this.toHierarchy = function(node) {
    var head = this.getHeadWord(node) 
    var ret = {}; 
    ret.name=head.word;
    ret.size=head.count;

    if(node.words.length>0 || node.children.length>0)
    {
      ret.children = node.words.map(w => r = {"name":w.word, "size":w.count}).concat(
        node.children.map(c => this.toHierarchy(c))
      );
    }
    return ret;
  };
  this.keepIfDescendantContains = function(node, filter) {
    node.children = node.children.map(c => this.keepIfDescendantContains(c, filter)).filter(c => c)
    
    if(node.children.length==0 && node.words.map(w => w.word).filter(w => w.match(filter)).length == 0)
      return null;
    return node;
  };

  this.keepFirstWordIfNotContains = function(node, filter) {
    if(node.words.map(w => w.word).filter(w => w.match(filter)).length == 0)
      node.words = node.words.slice(0,1)
    node.children.forEach(c => this.keepFirstWordIfNotContains(c, filter));
  };
  this.keepWordsOnSize = function(node, min, max) {
    node.words = node.words.filter(w => w.count>=min && (!max || w.count<=max))
    node.children.forEach(c => this.keepWordsOnSize(c, min, max));
  };

  this.popupWords = function(node, ascendants = []) {
    node.childrenWords = [];
    node.children.forEach(c => this.popupWords(c, ascendants.concat(node)))
    var myWordsInAncestors = ascendants.flatMap(a => a.words).distinct().filter(w => node.words.includes(w));
    //Removing words in node list if they are going to be pushed up
    node.words = node.words.filter(w => !myWordsInAncestors.includes(w));    
    
    //Dealing with children words that has beeb pished to me
    var childrenWordsToPopUp = node.childrenWords.filter(w => myWordsInAncestors.includes(w.name))
    var childrenWordsToStay = node.childrenWords.filter(w => !myWordsInAncestors.includes(w.name))

    var wordsToPopUp = myWordsInAncestors.map(w => r = {"name":w, "size":node.size, "children":[], "words":[]})
                     .concat(childrenWordsToPopUp)

    //Adding leaf childrens for children words not going to parents
    node.children = node.children.concat(childrenWordsToStay) 
    //Adding word to parent 
    if(ascendants.length>0) {
      var par = ascendants[ascendants.length-1]
      wordsToPopUp.forEach(wpop => {
        var same = par.childrenWords.filter(w => w.name === wpop.name)
        if(same[0]) {
          same.size = same.size + wpop.size;
        }
        else {
          par.childrenWords.push(wpop)
        }
      }) 
    }

  };
  this.toLeafOnlyHierarchy = function(node, level = 10) {
    var ret = {}; 
    ret.name=node.name;
    ret.size=node.size;

    if(node.children == 0)
    	ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "size":node.size})
    else
        ret.children = []

    ret.children = ret.children.concat(node.children.map(c => this.toLeafOnlyHierarchy(c, level - 1)))
    ret.name = ret.name// + node.words.map(w => w).join(", ")
    return ret;
  };
  this.toFullHierarchy = function(node) {
    var ret = {}; 
    ret.name=node.name;
    ret.size=node.size;

    ret.children = node.words.map(w => r = {"name":w, "docCount":node.size , "size":node.size})

    ret.children = ret.children.concat(node.children.map(c => this.toFullHierarchy(c)))
    return ret;
  };
  this.slice = function(node, hierarchy, startLevel, endLevel, minSize) {
    var canSplit = node.children.length>1 && node.children.filter(c => c.size<minSize).length == 0
    if(node.hierarchy.length < hierarchy.length) {
      var child = node.children.filter(c => c.hierarchy.slice(0, node.hierarchy.length+1).join(",") === hierarchy.slice(0, node.hierarchy.length+1).join(","))[0]
      return this.slice(child, hierarchy, startLevel, endLevel, sizeFrom);
    }
    else if(node.hierarchy.length==hierarchy.length && node.hierarchy.length<startLevel && canSplit)
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":[]
              , "children":node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel))}
    else if(node.hierarchy.length<startLevel && canSplit)
      return node.children.flatMap(c => this.slice(c, hierarchy, startLevel, endLevel, sizeFrom))
    else
    {
      return {"hierarchy":node.hierarchy, "name":node.name, "size":node.size, "words":node.words
              , "children":node.hierarchy.length<endLevel && canSplit?node.children.map(c => this.slice(c, hierarchy, startLevel, endLevel, sizeFrom)):[]}
    }
  }
    /*/If text matches one of node words we retain all the words except for 
    if(data.name.match(filtre)) 
      return data;
    //If a child matches the search then we return the matching child and single leaf brothers
    if(data.children && data.children.filter(c => c.name.match(filtre)).length>0) {
      data.children = data.children.filter(c => c.name.match(filtre) || !c.children)
      return data;
    }
    if(data.children) {
      var filtChildren = data.children.map(c => filterNodes(c, filtre)).filter(c => c);
      if(filtChildren.length > 0) {
        if(filtChildren.filter(c => !c.children).length == 0)
          filtChildren = filtChildren.concat(data.children.filter(c => !c.children).slice(0,1))
        data.children = filtChildren
        return data;
      }
    }
    return null; 
  }*/

}

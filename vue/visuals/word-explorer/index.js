var svg = d3.select("svg"),
    margin = 20,
    diameter = +svg.attr("width"),
    g = svg.append("g").classed("circle", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gText = svg.append("g").classed("text", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gAction = svg.append("g").classed("action", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

var color = d3.scaleLinear()
    .domain([-1, 5])
    .range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
    .interpolate(d3.interpolateHcl);

var pack = d3.pack()
    .size([diameter - margin, diameter - margin])
    .padding(2);


function renderTree(r) {
    root = d3.hierarchy(r)
        .sum(function(d) { return d.size; })
        .sort(function(a, b) { return b.data.id - a.data.id; });
 
    var nodes = pack(root).descendants();
    find
    nodes.forEach(n => {if(oldPositions[n.data.id]) {n.from = oldPositions[n.data.id]} else {
                                                                                              var par = visibleParentOf(n.data.id, root);
                                                                                              if(par.from) par = par.from;
                                                                                              n.from = {"x":par.x, "y":par.y, r:1}}})
    oldPositions = {}
    nodes.forEach(n => oldPositions[n.data.id]={"x":n.x, "y":n.y,"r":n.r}); 

    if(!currentFocus)
      setFocus(root, raw.root);
    else {
      var f= visibleParentOf(currentFocus.data.id, root);
      setFocus(f, raw.root);
    }

    var circUpdate = g.selectAll("circle").data(nodes, d => d.data.id);
    var circEnter = circUpdate.enter()
       .append("circle")
       .on("click", function(d) { if (currentFocus !== d) zoom(d), d3.event.stopPropagation(); })
       .on("mouseover", function(d) { showOptions(d, true) })

    removedCircle = circUpdate.exit()
        .classed("in", false)

    circle = circUpdate.merge(circEnter)
        .attr("class", function(d) { return d.parent ? d.children ? "node" : "node node--leaf" : "node node--root"; })
        .style("fill", function(d) { return d.children ? color(d.depth) : null; })
        .classed("in", true)
        .style("display", "inline")

    var textUpdate = gText.selectAll("text").data(nodes, d => d.data.id)
    var textEnter = textUpdate.enter()
        .append("text")
        .style("display", "inline")
        .attr("class", "label")

    removedText = textUpdate.exit()
        .classed("in", false)

    textEnter.append("tspan").classed("name", true).attr("x", 0);
    textEnter.append("tspan").classed("value",true).attr("x", 0).attr("dy", "1em");

    nodeText = textUpdate.merge(textEnter)
        .style("fill-opacity", function(d) { return d !==root && (d.parent === root || d.parent.parent === root)? 1 : 0; })
        .style("display", function(d) { return d!==root && (d.parent === root || d.parent.parent === root)? "inline" : "none"; })
        .classed("out", false)

    nodeText.selectAll("tspan.name").text(function(d, i, g) { return d.data.name; });
    nodeText.selectAll("tspan.value").text(function(d, i, g) { return d.data.noCount?"":d.data.size;});
   
    svg
        .style("background", color(-1))
        .on("click", function() { zoom(root); });

    init(currentFocus); 
    drawPhrases(currentFocus);
}

function visibleParentOf(id, node) {
  if(node.data.id === id)
   return node;
  if(node.children) {
    var closer = node.children.filter(c => id.startsWith(c.data.id))[0];
    if(!node.data.childrenHidden && closer)
      return visibleParentOf(id, closer);
    }
  return node;
}
function getFocus(id, node) {
  if(node.hierarchy.join(",") === id)
   return [node];
  return [node].concat(node.children.filter(c => id.startsWith(c.hierarchy.join(","))).flatMap(c => getFocus(id, c)));
}

function setFocus(node, root) {
  currentFocus = node; 
  var path = getFocus(node.data.id, root)
  var update = d3.select("#path-container").selectAll("div.path-part").data(path, d => d.hierarchy.join(",")); 
  var enter = update.enter();
  var exit = update.exit(); 

  var cont = enter.append("div")
    .classed("path-part", true)
    .on("click", (d)=> {toggleNode(d); d3.event.stopPropagation();})

  cont.append("div").classed("path-part-bullet", true).html("&nbsp;");
  cont.append("span").classed("path-part-text", true);

  exit.remove();
  update.merge(enter)
    .selectAll("span.path-part-text")
    .text(d => d.name);
    
}
function init(focus) {
  var transition = d3.transition("in")
      .duration(2000)
      .tween("entering", function(d) {
        var i = d3.interpolateNumber(0, 1);
        var newView = [currentFocus.x, currentFocus.y, currentFocus.r * 2 + margin];
        var oldView = view?view:newView;
        return function(t) { move(newView, i(t), oldView); };
      });
  transition.selectAll("text")
    .filter(function(d) { return d.parent && ((d.parent === currentFocus && !d.children) || d.parent.parent === currentFocus || this.style.display === "inline"); })
      .style("fill-opacity", function(d) { return (d.parent === currentFocus &&  !d.children) || d.parent.parent === currentFocus? 1 : 0; })
      .on("start", function(d) { if ((d.parent === currentFocus &&  !d.children) || d.parent.parent === currentFocus) this.style.display = "inline"; })
      .on("end", function(d) { if ((d.parent !== currentFocus ||  d.children) && d.parent.parent!==currentFocus) this.style.display = "none"; });

  d3.transition("out")
      .duration(500)
      .tween("leaving", function(d) {
        var i = d3.interpolateNumber(0, 1);
        return function(t) { leave([currentFocus.x, currentFocus.y, currentFocus.r * 2 + margin], i(t)); };
      }).on("end", function(d) { removedCircle.remove(); removedText.remove() });
;
  
}
function showOptions(d, show) {
  var data = show?[d]:[]
  var update = gAction.selectAll("circle").data(data);
  var enter = update.enter().append("circle")
                .classed("node-option", true)
                .on("click", (d)=> {showOptions(d, false); toggleNode(d); d3.event.stopPropagation();})
  update.exit().remove();

  var v = view;
  var k = diameter / v[2]; 

  update.merge(enter)
     .attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - d.r - v[1]) * k + ")"; })
     .attr("r", function(d) { return d.r/10 * k; });

}

function toggleNode(d) {
  if(d.data.childrenHidden) {
    expandNode(d);
  } else if(d.children.length>0) {
    collapseNode(d);
  } else {
    alert("fix me!");
  }
}

function expandNode(d) {
 if(!expandedNodes.includes(d.data.id)) {
   if(collapsedNodes.includes(d.data.id)) 
     collapsedNodes.splice(collapsedNodes.indexOf(d.data.id), 1);
   expandedNodes.push(d.data.id);
   render();
 } 
}

function collapseNode(d) {
 if(!collapsedNodes.includes(d.data.id)) {
   if(expandedNodes.includes(d.data.id))
     expandedNodes.splice(expandedNodes.indexOf(d.data.id), 1);
   collapsedNodes.push(d.data.id);
   render();
 } 
}


function zoom(d) {
  setFocus(d, raw.root);
  var transition = d3.transition()
      .duration(d3.event.altKey ? 7500 : 750)
      .tween("zoom", function(d) {
        var i = d3.interpolateZoom(view, [currentFocus.x, currentFocus.y, currentFocus.r * 2 + margin]);
        return function(t) { zoomTo(i(t)); };
      });
  drawPhrases(d);
  transition.selectAll("text")
    .filter(function(d) { return d.parent && ((d.parent === currentFocus && !d.children) || d.parent.parent === currentFocus || this.style.display === "inline"); })
      .style("fill-opacity", function(d) { return (d.parent === currentFocus &&  !d.children) || d.parent.parent === currentFocus? 1 : 0; })
      .on("start", function(d) { if ((d.parent === currentFocus &&  !d.children) || d.parent.parent === currentFocus) this.style.display = "inline"; })
      .on("end", function(d) { if ((d.parent !== currentFocus ||  d.children) && d.parent.parent!==currentFocus) this.style.display = "none"; });

} 


function zoomTo(v) {
  var k = diameter / v[2]; view = v;
  circle
      .attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; })
      .attr("r", function(d) { return d.r * k; });
  nodeText.attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; });
}

function move(v, t, oldV) {
  var k = diameter / v[2]; view = v;
  var oldK = diameter / oldV[2];
  circle
      .attr("transform", function(d) { return "translate(" + d3.interpolateNumber((d.from.x - oldV[0]) * oldK, (d.x - v[0]) * k)(t) + "," + d3.interpolateNumber((d.from.y - oldV[1]) * oldK, (d.y - v[1]) * k)(t) + ")"; })
      .attr("r", function(d) { return d3.interpolateNumber(d.from.r * oldK , d.r * k)(t); });
  nodeText
      .attr("transform", function(d) { return "translate(" + d3.interpolateNumber((d.from.x - oldV[0]) * oldK, (d.x - v[0]) * k)(t) + "," + d3.interpolateNumber((d.from.y - oldV[1]) * oldK, (d.y - v[1]) * k)(t) + ")"; })
}

function leave(v, t) {
  var k = diameter / v[2]; view = v;
  removedCircle
      .attr("r", function(d) { return d3.interpolateNumber(d.r * k, 1)(t); });
  removedText
}


function drawPhrases(d) {
  var updatePhrase = d3.select("#phrasesContainer").selectAll("div.phrase").data(d.data.phrases);
  var enterPhrase = updatePhrase.enter().append("div");
  var exitPhrase = updatePhrase.exit();
  enterPhrase.classed("rowdiv phrase", true).style("display","flex");
  enterPhrase.append("div").classed("cell phrase-bullet", true).html("&nbsp;&nbsp;");
  enterPhrase.append("div").classed("cell phrase-text", true);
  enterPhrase.merge(updatePhrase)
    .style("display", "flex").select("div.phrase-text").text(d => d+"...")

  exitPhrase.style("display", "none");
}

function render() {
  var firstLoad = !hierarchy
  if(firstLoad) {
     raw = new WordTree().fromPath("phrase_clusters.json", refreshTree);
  }
  else {
    refreshTree(raw);
  }
}

function refreshTree(tree) {
  var firstLoad = !hierarchy
  var sliderChanged = firstLoad || d3.select("#slider-size-div").datum().from|0 != sizeFrom || d3.select("#slider-size-div").datum().to|0 != sizeTo
                               || d3.select("#slider-ratio-div").datum().from|0 != ratioFrom || d3.select("#slider-ratio-div").datum().to|0 != ratioTo 
                               || d3.select("#slider-level-div").datum().from|0 != levelFrom || d3.select("#slider-level-div").datum().to|0 != levelTo 
  var filterChanged = d3.select("#filtre").property("value")!=filterValue
  if(firstLoad || sliderChanged || filterChanged) {
    sizeFrom = firstLoad?10:d3.select("#slider-size-div").datum().from|0;
    sizeTo = firstLoad?null:d3.select("#slider-size-div").datum().to|0;
    ratioFrom = firstLoad?0:d3.select("#slider-ratio-div").datum().from/100;
    ratioTo = firstLoad?null:d3.select("#slider-ratio-div").datum().to/100;
    levelFrom = firstLoad?3:d3.select("#slider-level-div").datum().from|0;
    levelTo = firstLoad?6:d3.select("#slider-level-div").datum().to|0;
    filterValue = d3.select("#filtre").property("value");
  }
  var ratioRange = tree.getRatioRange(tree.root)
  if(firstLoad) ratioTo = ratioRange.max
  if(firstLoad) sizeTo = tree.root.size

  var toRender = tree.slice(tree.root, tree.root.hierarchy, levelFrom, levelTo, sizeFrom, sizeTo, ratioFrom, ratioTo, filterValue, expandedNodes, collapsedNodes); 
  hierarchy = tree.toLeafOnlyHierarchy(toRender);

  //Drawing filters
  if(firstLoad) {
    sizeMin = 1;
    sizeMax = sizeTo;
    sizeData = {"min":sizeMin, "max":sizeMax, "from":sizeFrom, "to":sizeTo
         , "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
    ratioMin = 0;
    ratioMax = ratioTo;
    ratioData = {"min":ratioMin*100, "max":(ratioMax*100)|0, "from":ratioFrom*100, "to":(ratioTo*100)|0
         , "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
    levelMin = 2
    levelMax = 10
    levelData = {"min":levelMin, "max":levelMax, "from":levelFrom|0, "to":levelTo|0
         , "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
  }
  else {
    sizeData.from = sizeFrom;
    sizeData.to = sizeTo;

    ratioData.from = (ratioFrom*100)|0;
    ratioData.to = (ratioTo*100)|0;

    levelData.from = levelFrom|0;
    levelData.to = levelTo|0;
  }
  d3.select("#slider-size-div").call(epislider, [sizeData]);
  d3.select("#slider-ratio-div").call(epislider, [ratioData]);
  d3.select("#slider-level-div").call(epislider, [levelData]);
  renderTree(hierarchy);
}

var hierarchy;
var raw;
var root;
var filterValue;
var sizeFrom;
var sizeTo;
var sizeMin;
var sizeMax;
var sizeData;
var ratioFrom;
var ratioTo;
var ratioMin;
var ratioMax;
var levelData;
var levelFrom;
var levelTo;
var levelMin;
var levelMax;
var ratioData;
var view;
var currentFocus;
var nodeCircle = null;
var nodeText = null;
var circle = null;
var oldPositions = {};
var removedCircle;
var removedText;
var expandedNodes = [];
var collapsedNodes = [];
render();


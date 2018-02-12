

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
function getNode(id, node) {
  if(node.hierarchy.join(",") === id)
   return node;
  return getNode(id, node.children.filter(c => id.startsWith(c.hierarchy.join(",")))[0]);
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

function isTextVisible(d) {
  return (d === currentFocus && d.children && (d.children[0].children))
   || d.parent === currentFocus && (!d.children || currentFocus.children.length > 10 || d.children[0].children) 
   || (d.parent && d.parent.parent === currentFocus && currentFocus.children && currentFocus.children.length <= 10  && (!d.children || d.children[0].children))
   || (d.parent && d.parent.parent && currentFocus.children && currentFocus.children.length <= 10 && d.parent.parent.parent === currentFocus)
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
  transition.selectAll("#wordtree text")
    .filter(function(d) { return  isTextVisible(d) || this.style.display === "inline"; })
    .style("fill-opacity", function(d) { return isTextVisible(d)? 1 : 0; })
    .on("end", function(d) { if(isTextVisible(d)) this.style.display = "inline"; else this.style.display = "none"; });

  d3.transition("out")
      .duration(500)
      .tween("leaving", function(d) {
        var i = d3.interpolateNumber(0, 1);
        return function(t) { leave([currentFocus.x, currentFocus.y, currentFocus.r * 2 + margin], i(t)); };
      }).on("end", function(d) { removedCircle.remove(); removedText.remove() });
;
  
}
function showOptions(d, show) {
  var canCollapse = !d.data.leaf;
  var isExpanded = !d.data.childrenHidden;
  var isSelected =  selectedNodes.includes(d.data.id); 
  var angleExpand, angleSelect;

  if(!canCollapse) {
    angleSelect = 0; 
  } else {
    angleExpand = -Math.PI/20
    angleSelect = Math.PI/20
  } 
  var data = show?[d]:[]
  //Show expandCollapse 
  if(canCollapse) {
    var update = gAction.selectAll("circle.node-option-expand").data(data);
    var enter = update.enter().append("circle")
                  .classed("node-option", true)
                  .classed("node-option-expand", true)
                  .on("click", (d)=> {showOptions(d, false); toggleNode(d); d3.event.stopPropagation();})
    update.exit().remove();
  
    var v = view;
    var k = diameter / v[2]; 
    update.merge(enter)
       .attr("transform", function(d, i) { return "translate(" + (d.x + d.r * Math.sin(angleExpand) - v[0]) * k + "," + (d.y - d.r*Math.cos(angleExpand) - v[1]) * k + ")"; })
       .attr("r", function(d) { return d.r/10 * k; })
       .attr("fill", isExpanded?"url(#collapse)":"url(#expand)");
    }
  //Show select
  var update = gAction.selectAll("circle.node-option-select").data(data);
  var enter = update.enter().append("circle")
                .classed("node-option", true)
                .classed("node-option-select", true)
                .on("click", (d)=> {showOptions(d, false); toggleSelection(d); d3.event.stopPropagation();})
  update.exit().remove();

  var v = view;
  var k = diameter / v[2]; 
  update.merge(enter)
     .attr("transform", function(d, i) { return "translate(" + (d.x + d.r * Math.sin(angleSelect) - v[0]) * k + "," + (d.y - d.r*Math.cos(angleSelect) - v[1]) * k + ")"; })
     .attr("r", function(d) { return d.r/10 * k; })
     .attr("fill", isSelected?"url(#selected)":"url(#unselected)");
}

function toggleNode(d) {
  if(d.data.childrenHidden) {
    expandNode(d);
  } else if(d.children.length>0) {
    collapseNode(d);
  }
}

function toggleSelection(d) {
 if(selectedNodes.includes(d.data.id))
     selectedNodes.splice(selectedNodes.indexOf(d.data.id), 1);
 else 
     selectedNodes.push(d.data.id);
 d3.select("#selection").property("value", selectedNodes.join(";"));
 render();
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
    .filter(function(d) { return  isTextVisible(d) || this.style.display === "inline"; })
    .style("fill-opacity", function(d) { return isTextVisible(d)? 1 : 0; })
    .on("end", function(d) { if(isTextVisible(d)) this.style.display = "inline"; else this.style.display = "none"; });

} 

function isTextCentered(d) {
 return !d.children || (!isTextVisible(d.children[0]) && (!d.children[0].children || !isTextVisible(d.children[0].children[0])));
} 

function zoomTo(v) {
  var k = diameter / v[2]; view = v;
  circle
      .attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1]) * k + ")"; })
      .attr("r", function(d) { return d.r * k; });
  nodeText.attr("transform", function(d) { return "translate(" + (d.x - v[0]) * k + "," + (d.y - v[1] - (isTextCentered(d)?0:d.r*0.8)) * k + ")"; });
}

function move(v, t, oldV) {
  var k = diameter / v[2]; view = v;
  var oldK = diameter / oldV[2];
  circle
      .attr("transform", function(d) { return "translate(" + d3.interpolateNumber((d.from.x - oldV[0]) * oldK, (d.x - v[0]) * k)(t) + "," + d3.interpolateNumber((d.from.y - oldV[1]) * oldK, (d.y - v[1]) * k)(t) + ")"; })
      .attr("r", function(d) { return d3.interpolateNumber(d.from.r * oldK , d.r * k)(t); });
  nodeText
      .attr("transform"
       , function(d) { 
          return "translate(" 
             + d3.interpolateNumber((d.from.x - oldV[0]) * oldK, (d.x - v[0]) * k)(t) + "," 
             + d3.interpolateNumber((d.from.y - oldV[1] - (isTextCentered(d)?0:d.from.r*0.8)) * oldK, (d.y - v[1] - (isTextCentered(d)?0:d.r*0.8)) * k)(t) 
       + ")"; })
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

function drawSelectedClusters() {
  if(selectedNodes.length  == 0)
    d3.select("#selectedFrame").style("display","none");
  var updateCluster = d3.select("#selectedClusterContainer").selectAll("tr.cluster").data(selectedNodes.map(id => getNode(id, raw.root)), n => n.hierarchy.join(","));
  var enterCluster = updateCluster.enter().append("tr");
  var exitCluster = updateCluster.exit();
  enterCluster.classed("cluster", true);
  enterCluster.append("td").classed("td-bullet", true).html("&nbsp;&nbsp;");
  enterCluster.append("td").classed("cluster-name", true).text(d => d.name);
  enterCluster.append("td").classed("cluster-size", true).text(d => d.size);
  enterCluster.append("td").classed("cluster-topWOrds", true).text(d => d.words.join(", "));

  exitCluster.remove();
  if(selectedNodes.length > 0)
    d3.select("#selectedFrame").style("display",null);
}

function render() {
  //setting default sizes
  var width = d3.select("#svg-container").node().parentNode.getBoundingClientRect().width;
  var svgWidth, columnWidth;
  
  if(width>600) {
    svgWidth = (width*0.6)|0
    columnWidth = (width*0.4-20)|0
  } else {
    svgWidth = width|0;
    columnWidth = (width)|0
  }
  d3.select("#svg-container").style("width", svgWidth+"px");
  d3.select("#wordtree").attr("width", svgWidth);
  d3.select("#wordtree").attr("height", svgWidth);
  d3.select("#right-column").style("width", columnWidth+"px");

  svg = d3.select("#svg-container > svg");
  margin = 20;
  diameter = +svg.attr("width");


  var firstLoad = !hierarchy
  if(firstLoad) {
    g = svg.append("g").classed("circle", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gText = svg.append("g").classed("text", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gAction = svg.append("g").classed("action", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

    pack = d3.pack()
      .size([diameter - margin, diameter - margin])
      .padding(2);

    color = d3.scaleLinear()
      .domain([-1, 5])
      .range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
      .interpolate(d3.interpolateHcl);

    raw = new WordTree().fromPath("phrase_clusters.json", refreshTree);
  }
  else {
    g = svg.select("g.circle").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gText = svg.select("g.text").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
    gAction = svg.select("g.action").attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

    pack = d3.pack()
      .size([diameter - margin, diameter - margin])
      .padding(2);

    refreshTree(raw);
    drawSelectedClusters();
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

  var toRender = tree.slice(tree.root, tree.root.hierarchy, levelFrom, levelTo, sizeFrom, sizeTo, ratioFrom, ratioTo, filterValue, expandedNodes, collapsedNodes, selectedNodes); 
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

function refreshSelection() {
  selectedNodes = d3.select("#selection").property("value").split(";");
  d3.select('#charger').style('display','none')
  render();
}

var svg;
var margin;
var diameter;
var g;
var gText;
var gAction;
var color = d3.scaleLinear()
var pack = d3.pack()
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
var selectedNodes = [];
render();
d3.select(window).on('resize', render); 

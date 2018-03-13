

function renderTree(r) {
    root = d3.hierarchy(r)
        .sum(function(d) { return d.size; })
        .sort(function(a, b) { return b.data.id - a.data.id; });
 
    var nodes = pack(root).descendants();
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
        .style("fill", function(d) { return d.children ? getColor(d.data.id, d.depth) : null; })
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
        .style("background", defaultColor(-1))
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
  var isSelected =  S.selectedNodes[d.data.id]; 
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
  if(!S.taggedNodes[d.data.id]) S.taggedNodes[d.data.id] = []

  var clusterTags = S.taggedNodes[d.data.id]
  var testTags = Object.keys(S.tags).map(k => S.tags[k])
                    .filter(t => t.default_on_select).sort((a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()))
                    .map(t => t.name);
  var missingTags = testTags.filter(t => !clusterTags.includes(t));
  if(missingTags.length>0) {
    //adding missing tags if missing
    missingTags.forEach(t => clusterTags.push(t))
    clusterTags.sort((a, b) => a.toLowerCase().localeCompare(b.toLowerCase()));
    S.selectedNodes[d.data.id] = true;
  }
  else {
    //Removing test tags from cluster
    testTags.forEach(t => clusterTags.splice(clusterTags.indexOf(t), 1))
    delete S.selectedNodes[d.data.id];
  }
 render();
}

function expandNode(d) {
 if(!S.expandedNodes[d.data.id]) {
   if(S.collapsedNodes[d.data.id]) 
     delete S.collapsedNodes[d.data.id];
   S.expandedNodes[d.data.id]=true;
   render();
 } 
}

function collapseNode(d) {
 if(!S.collapsedNodes[d.data.id]) {
   if(S.expandedNodes[d.data.id])
     delete S.expandedNodes[d.data.id];
   S.collapsedNodes[d.data.id]=true;
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
  //removedText
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

function drawSelectedClusters(tree) {
  var clusters = Object.keys(S.selectedNodes);
  var updateCluster = d3.select("#selectedClusterContainer").selectAll("tr.cluster").data(clusters.map(id => getNode(id, tree.root)), n => n.hierarchy.join(","));
  var enterCluster = updateCluster.enter().append("tr");
  var exitCluster = updateCluster.exit();
  enterCluster.classed("cluster", true);
  var tags = enterCluster.append("td").classed("td-tag", true);
  enterCluster.append("td").classed("cluster-name", true).text(d => d.name);
  enterCluster.append("td").classed("cluster-size", true).text(d => d.size);
  enterCluster.append("td").classed("cluster-topWOrds", true).text(d => d.words.join(", "));
  
  var allRows = updateCluster.merge(enterCluster)
  var tagsUpd = allRows.selectAll("td.td-tag").selectAll("span.tag").data(n => S.taggedNodes[n.hierarchy.join(",")]
                                                                     .map(tag => S.tags[tag].color)
                                                                     .map(c => `rgb(${c[0]},${c[1]},${c[2]})`)
                                                ,d => d)
  var tagsEnt = tagsUpd.enter().append("span").classed("tag fas fa-square", true).style("color", d => d).style("margin","1px");
  tagsUpd.exit().remove();

  exitCluster.remove();
  if(clusters.length  == 0)
    d3.select("#selectedFrame").style("display","none");
  else
    d3.select("#selectedFrame").style("display",null);
}

function initLayout() {
  //Adjusting sizes and changing layout based on screen size
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

  //Building toolbar
  //Save
  var saveButton = d3.select("div.toolbox").selectAll("span.save-button").data([1]).enter().append("span").classed("save-button", true);
  saveButton.classed("fa-stack", true).classed("fa-1x", true)
    .classed("toolbox-icon", true).classed("celldiv", true).style("margin", "0px -1px 0px -1px")
    .attr("title", "Save changes")
  saveButton.append("i").classed("fas fa-square fa-stack-2x", true)
  saveButton.append("i").classed("fas fa-save fa-stack-1x fa-inverse", true)
  saveButton.on("click", saveContext)
  //Clear all 
  var clearButton = d3.select("div.toolbox").selectAll("span.clear-button").data([1]).enter().append("span").classed("clear-button", true);
  clearButton.classed("fa-stack", true).classed("fa-1x", true)
    .classed("toolbox-icon", true).classed("celldiv", true).style("margin", "0px -1px 0px -1px")
    .attr("title", "Clear changes")
  clearButton.append("i").classed("fas fa-square fa-stack-2x", true)
  clearButton.append("i").classed("fas fa-eraser fa-stack-1x fa-inverse", true)
  clearButton.on("click", clearAll)
  //Category Selector 
  var brushButton = d3.select("div.toolbox").selectAll("span.brush-button").data([1]).enter().append("span").classed("brush-button", true);
  brushButton.classed("fa-stack", true).classed("fa-1x", true)
    .classed("toolbox-icon", true).classed("celldiv", true).style("margin", "0px -1px 0px -1px")
    .attr("title", "Selector")
  brushButton.append("i").classed("fas fa-square fa-stack-2x", true)
  brushButton.append("i").classed("fas fa-paint-brush fa-stack-1x fa-inverse", true)
  brushButton.on("click", toggleCategoryEditor)


  svg = d3.select("#svg-container > svg");
  margin = 20;
  diameter = +svg.attr("width");

  pack = d3.pack()
    .size([diameter - margin, diameter - margin])
    .padding(2);

  
}

function render(filtersFromStore, defaultFilters) {
  var filtersFromStore = (typeof filtersFromStore !== 'undefined') ? filtersFromStore : false;
  var defaultFilters = (typeof defaultFilters !== 'undefined') ? defaultFilters : false;


  //g = svg.append("g").classed("circle", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
  var gUpd = svg.selectAll("g.circle").data([1]);
  var gEnt = gUpd.enter().append("g").classed("circle", true);
  g = gUpd.merge(gEnt).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

  var gTextUpd = svg.selectAll("g.text").data([1]);
  var gTextEnt = gTextUpd.enter().append("g").classed("text", true);
  gText = gTextUpd.merge(gTextEnt).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

  var gActionUpd = svg.selectAll("g.action").data([1]);
  var gActionEnt = gActionUpd.enter().append("g").classed("action", true);
  gAction = gActionUpd.merge(gActionEnt).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

  //gText = svg.append("g").classed("text", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");
  //gAction = svg.append("g").classed("action", true).attr("transform", "translate(" + diameter / 2 + "," + diameter / 2 + ")");

 
  if(!raw)
      raw = new WordTree().fromPath("data/phrase_clusters.json", (tree) => {refreshTree(tree, filtersFromStore, defaultFilters);drawSelectedClusters(tree);});
  else {
    refreshTree(raw, filtersFromStore, defaultFilters);
    drawSelectedClusters(raw);
  }
}

function refreshTree(tree, filtersFromStore, defaultFilters) {
  var filtersFromStore = (typeof filtersFromStore !== 'undefined') ? filtersFromStore : false;
  var defaultFilters = (typeof defaultFilters !== 'undefined') ? defaultFilters : false;

  var filterChanged = (!defaultFilters && !filtersFromStore) && (
       (d3.select("#slider-size-div").datum().from|0)  != S.filters.size.from ||   (d3.select("#slider-size-div").datum().to|0) != S.filters.size.to
    || (d3.select("#slider-ratio-div").datum().from|0) != S.filters.ratio.from || (d3.select("#slider-ratio-div").datum().to|0) != S.filters.ratio.to 
    || (d3.select("#slider-level-div").datum().from|0) != S.filters.level.from || (d3.select("#slider-level-div").datum().to|0) != S.filters.level.to 
    || d3.select("#filtre").property("value")!=S.filters.search
    || d3.select("#filter-tag-div").datum().items.filter(t => t.selected != S.tags[t.name].withinSearch).length > 0
    || d3.select("#filter-tag-div").datum().noTagSelected != S.filters.tags.noTagSelected
    )

  //Setting filters default values
  if(defaultFilters) {
    S.filters.size.min = 1;
    S.filters.size.max = tree.root.size;
    S.filters.size.from = S.filters.size.min
    S.filters.size.to = S.filters.size.max

    var ratioRange = tree.getRatioRange(tree.root)
    S.filters.ratio.min = 0;
    S.filters.ratio.max = ratioRange.max * 100+1
    S.filters.ratio.from = S.filters.ratio.min;
    S.filters.ratio.to = S.filters.ratio.max;

    S.filters.level.min = 2;
    S.filters.level.max = 10;
    S.filters.level.from = 3;
    S.filters.level.to = 6;

    S.filters.tags.noTagSelected = true;
    
  }
  //Reloading from D3 datums 
  else if(!filtersFromStore && filterChanged) {
    S.filters.size.from = d3.select("#slider-size-div").datum().from|0;
    S.filters.size.to = d3.select("#slider-size-div").datum().to|0;
    S.filters.ratio.from = d3.select("#slider-ratio-div").datum().from|0;
    S.filters.ratio.to = d3.select("#slider-ratio-div").datum().to|0;
    S.filters.level.from = d3.select("#slider-level-div").datum().from|0;
    S.filters.level.to = d3.select("#slider-level-div").datum().to|0;
    S.filters.search = d3.select("#filtre").property("value");
    Object.keys(S.tags).forEach(t => S.tags[t].withinSearch = d3.select("#filter-tag-div").datum().items.filter(tt => tt.name == t)[0].selected);
    if(!S.filters.tags) S.filters.tags = {}; 
    S.filters.tags.noTagSelected = d3.select("#filter-tag-div").datum().noTagSelected;
  }
 
  //updating selected nodes based on selected categories
  Object.keys(S.selectedNodes).concat([])
     .filter(nodeId => !Boolean(S.taggedNodes[nodeId]) || S.taggedNodes[nodeId].filter(tag => S.tags[tag].withinSearch).length==0)
     .forEach(nodeId => delete S.selectedNodes[nodeId]); 
  Object.keys(S.taggedNodes)
     .filter(nodeId => S.taggedNodes[nodeId].filter(tag => S.tags[tag].withinSearch).length>0) 
     .forEach(nodeId => S.selectedNodes[nodeId] = true); 

  var toRender = tree.slice(tree.root, tree.root.hierarchy
                     , S.filters.level.from, S.filters.level.to, S.filters.size.from, S.filters.size.to, S.filters.ratio.from/100, S.filters.ratio.to/100, S.filters.search
                     , Object.keys(S.expandedNodes), Object.keys(S.collapsedNodes), Object.keys(S.selectedNodes), S.taggedNodes, S.tags, S.filters.tags.noTagSelected
                  ); 
  hierarchy = tree.toLeafOnlyHierarchy(toRender);

  d3.select("#slider-size-div").call(epislider, [S.filters.size]);
  d3.select("#slider-ratio-div").call(epislider, [S.filters.ratio]);
  d3.select("#slider-level-div").call(epislider, [S.filters.level]);

  var catItems = Object.keys(S.tags).sort((a, b)=> a.toLowerCase().localeCompare(b.toLowerCase())).map(k => { return {"name":S.tags[k].name, "selected":S.tags[k].withinSearch, "color":`rgb(${S.tags[k].color[0]}, ${S.tags[k].color[1]}, ${S.tags[k].color[2]})`}});
  var catData = {"name":"tags", "noTagSelected":S.filters.tags.noTagSelected, "items":catItems} 

  if(epifilters) {
    epifilters.refresh(d3.select("#filter-tag-div"), [catData]);
    epifilters.render();
  } else {
    epifilters = new epifilter(d3.select("#filter-tag-div"), [catData]);
  }
  renderTree(hierarchy);
}

function loadContext(callback) {
  d3.json("data/context.json", function(error, context) {
    if(error){
      console.warn(error)
      callback(null, "Cannot load context file");
    } else {
      if(context.filters) {
         callback(context, null);
      } else {
         callback(null, "Cannot interpret context file");
      }
    }

  });
 

}

function saveContext() {
  download("context.json", JSON.stringify(S));
}

function clearAll() {
  S = baseS;
  hierarchy = null;
  currentFocus = null;
  render(false, true);
}

function toggleCategoryEditor() {
  renderCategories(true)
}
function renderCategories(toggle) {
  //Category Editor 
  var updCatEd = d3.select("div.toolbox").selectAll("div.category-editor").data([1])
  var entCatEd = updCatEd.enter().append("div").classed("category-editor", true);
  var header = entCatEd.append("table").classed("category-list", true);
  var catEd = updCatEd.merge(entCatEd);
  if(toggle && catEd.style("display")  != "none" && updCatEd.size()>0) {
    catEd.style("display", "none")
  }
  else {
    var rect = d3.select("div.toolbox span.brush-button").node().getBoundingClientRect();
    catEd
     .style("top", (rect.bottom+ window.scrollY)+"px")
     .style("left", (rect.left+ window.scrollX)+"px")
     .style("display", "block")

    var cats = Object.keys(S.tags).map(s => S.tags[s])
    cats.push({"name":"","default_on_click":false,"default_on_select":false,"color":randomColor(), "temp":true});
    var rowUpd = catEd.select("table").selectAll("tr.category-row").data(cats, c => c.name);
    var rowEnt = rowUpd.enter().append("tr").classed("category-row", true).attr("name", d => d.name);
    var rowEx = rowUpd.exit().remove();
    rowEnt.append("td").classed("category-editor-cell category-name", true)
      .append("input").property("value", t => t.name).on("change", function(d) {categoryChanged(d.name,"name", d3.select(this).property("value"))});
    rowEnt.append("td").classed("category-editor-cell category-color", true)
      .append("input").attr("type", "color").on("change", function(d) {categoryChanged(d.name, "color", d3.select(this).property("value"))});
    rowEnt.append("td").classed("category-editor-cell category-on-click", true)
      .append("span").classed("category-editor-button fa-hand-pointer", true).on("click", function(d) {categoryChanged(d.name, "on-click", !d.default_on_click)});
    rowEnt.append("td").classed("category-editor-cell category-on-select", true)
      .append("span").classed("category-editor-button far", true).on("click", function(d) {categoryChanged(d.name, "on-select", !d.default_on_select)});
    var actions = rowEnt.append("td").classed("category-editor-cell category-action", true)
    actions.filter(d => !d.temp).append("span").classed("category-editor-button far fa-trash-alt", true)
      .on("click", function(d) {categoryChanged(d.name, "delete")});
    var rows = rowUpd.merge(rowEnt);
    rows.select("td.category-name input").property("value", d => d.name);
    rows.select("td.category-color input").property("value", d => "#"+toHex(d.color[0])+toHex(d.color[1])+toHex(d.color[2]))
    rows.select("td.category-on-click span").classed("fas", d => d.default_on_click).classed("far", d => !d.default_on_click)
    rows.select("td.category-on-select span").classed("fa-check-square", d => d.default_on_select).classed("fa-square", d => !d.default_on_select)
    rows.sort((a, b) => a.name===""?true:(b.name===""?false:a.name.toLowerCase().localeCompare(b.name.toLowerCase())));
  }
}

var lastRandom = null;
function randomColor() {
  var r = Math.floor(Math.random() * Math.floor(8)) * 16;
  var g = Math.floor(Math.random() * Math.floor(8)) * 16;
  var b = Math.floor(Math.random() * Math.floor(8)) * 16;
  if(r > g && r > b) r = r + 128;
  if(g > r && g > b) g = g + 128;
  if(b > g && b > r) b = b + 128;
  lastRandom =  [r, g, b];
  return lastRandom;
}

function categoryChanged(name, action, value) {
  //var row = d3.select(`div.category-editor table tr.category-row[name=${name}]`)
  if(action === "name" && value === "")  {
    alert("Cannot give an empty name to a category");
  }
  
  if(name === "") {
    var newNameBase = action === "name"?value:"New Category"
    var newName = newNameBase;
    var i = 1;
    while(S.tags[newName]) {newName = newNameBase+" "+i;i++;}
    name = newName
    S.tags[newName] = {"name":newName,"default_on_click":false,"default_on_select":false,"color":lastRandom,"withinSearch":true }
    if(action === "name"){
      render(true, false);
      renderCategories(false); 
      return;
    }
  }
  if(action === "name") {
    if(S.tags[value]) alert(`Cannot rename category ${name===""?"<new>":name} as ${value} since other category is already named like that`);
    else if(name === "")
      S.tags[value].name = value;
    else {
      S.tags[value] = S.tags[name];
      delete S.tags[name];
      S.tags[value].name = value;
    }
  } else if(action === "color") {
    S.tags[name].color[0] = d3.rgb(value).r
    S.tags[name].color[1] = d3.rgb(value).g
    S.tags[name].color[2] = d3.rgb(value).b
  } else if(action === "on-click") {
    S.tags[name].default_on_click = value    
  } else if(action === "on-select") {
    S.tags[name].default_on_select = value    
  } else if(action === "delete") {
      delete S.tags[name];
  }
  refreshColors();
  render(true, false);
  renderCategories(false); 
}

function refreshColors() {
   Object.keys(colors).forEach(k => delete colors);
   Object.keys(S.tags).map(k => S.tags[k]).forEach(t => {
     var c = d3.hsl(`rgb(${t.color[0]}, ${t.color[1]}, ${t.color[2]})`)
     colors[t.name] = d3.scaleLinear()
       .domain([1, 10])
       .range([`hsl(${c.h},${c.s*100}%,${c.l*100}%)`, `hsl(${c.h},${c.l*50}%,${c.l*50}%)`])
       .interpolate(d3.interpolateHcl); 
   });  
   defaultColor =  d3.scaleLinear()
       .domain([1, 10])
       .range(["hsl(152,80%,80%)", "hsl(228,30%,40%)"])
       .interpolate(d3.interpolateHcl);
}

function getColor(clusterid, depth) {
  if(S.taggedNodes[clusterid] && Object.keys(S.taggedNodes[clusterid]).length>0) {
    return colors[S.taggedNodes[clusterid][0]](depth);
  }
  else
    return defaultColor(depth);
}

function toHex(c) {
    var hex = c.toString(16);
    return hex.length == 1 ? "0" + hex : hex;
}
function download(filename, text) {
    var exists = d3.select("#saveas").size()>0
    var pom = exists?document.getElementById("saveas"):document.createElement('a');

    pom.setAttribute('id', "saveas");
    pom.setAttribute('href', 'data:text/plain;charset=utf-8,' + encodeURIComponent(text));
    pom.setAttribute('download', filename);

    if (document.createEvent) {
        var event = document.createEvent('MouseEvents');
        event.initEvent('click', true, true);
        pom.dispatchEvent(event);
    }
    else {
        pom.click();
    }
}

var svg;
var margin;
var diameter;
var g;
var gText;
var gAction;
var colors = {}
var defaultColor
var pack = d3.pack()
var hierarchy;
var raw;
var root;
var view;
var currentFocus;
var nodeCircle = null;
var nodeText = null;
var circle = null;
var oldPositions = {};
var removedCircle;
var removedText;
var epifilters;
var baseS = 
{"expandedNodes":{}
  , "collapsedNodes":{}
  ,"selectedNodes":{}
  ,"tags":{
    "selected":{"name":"selected",	"default_on_click":false, 	"default_on_select":true, "color":[128, 128, 128], "withinSearch":true}
    ,"visited":{"name":"visited",	"default_on_click":true, 	"default_on_select":false,  "color":[128, 128, 0], "withinSearch":true}
   }
  ,"taggedNodes":{}
  ,"filters":{
    "search":null
    ,"size":{"from":null, "to":null, "min":null, "max":null, "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
    ,"ratio":{"from":null, "to":null, "min":null, "max":null, "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
    ,"level":{"from":null, "to":null, "min":null, "max":null, "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17}
    ,"tags":{"noTagSelected":true}
  }
}
var S = baseS; 
initLayout();
loadContext((context, error) => {
  var loadFromStore = false
  if(!error) {
    S = context;
    loadFromStore  = true;
  }
  refreshColors(); 
  render(loadFromStore, false);
});
d3.select(window).on("resize", initLayout);


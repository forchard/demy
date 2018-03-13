

function epifilter(selection, data) {
  this.selection = selection;
  this.data = data?[].concat(data):[];
  var theFilter = this;

//  this.usedWidth = function(d) {return d.width - 2*d.padding};
//  this.setScale = function(d) {d.scale = d.scale?d.scale:d3.scaleLinear().domain([d.min, d.max]).range([d.padding, d.width - d.padding]);return this};
//  this.setAxis = function(d) {d.axis = d.axis?d.axis:d3.axisBottom().scale(d.scale).ticks(3);return this};
  this.refresh = function(selection, data) {
    this.selection = selection;
    this.data = data?[].concat(data):[];
  };
  this.render = function() {
    //Making data and selection on the same size. If data not provided then set to default
    var defaultData = [];
    //this.setScale(defaultData).setAxis(defaultData);
    var selSize = this.selection.size();
    for(var i=this.data.length; i<selSize; i++) this.data.push({"name":"filter_"+i, "items":[]});
    this.data = this.data.slice(0, selSize);

    this.selection.data(this.data);
    //Base canvas
    var canvasUpd = this.selection.selectAll("div.epifilter-canvas").data(this.data, d=>d.name);
    var canvasEnter = canvasUpd.enter().append("div").classed("epifilter-canvas", true);
    canvasUpd.exit().remove;
    var canvas = canvasUpd.merge(canvasEnter);

    //Adding all category element
    var allUpd = canvas.selectAll("div.epifilter-all").data(d => [d.items]);
    var allEnter = allUpd.enter().append("div").classed("epifilter-all epifilter-elem", true).attr("title", "All categories");
    var allItems = allUpd.merge(allEnter); 
    allEnter.append("span").classed("epifilter-all-check epifilter-check", true).classed("far", true);
    allEnter.append("span").classed("epifilter-all-title", true).text("All Categories");
    allEnter.on("click", this.toggleAll);
    allItems.selectAll("span.epifilter-all-check")
     .classed("fa-check-square", function(d) { return d3.select(this.parentNode).datum().filter(f => !f.selected).length==0})
     .classed("fa-square", function(d) {return d3.select(this.parentNode).datum().filter(f => !f.selected).length>0});

    //Adding no category element
    var nocatUpd = canvas.selectAll("div.epifilter-nocat").data(d => [d]);
    var nocatEnter = nocatUpd.enter().append("div").classed("epifilter-nocat epifilter-elem", true);
    var nocatItems = nocatUpd.merge(nocatEnter); 
    nocatEnter.append("span").classed("epifilter-nocat-check epifilter-check", true).classed("far", true);
    nocatEnter.append("span").classed("epifilter-nocat-title", true).text("No tag");
    nocatEnter.on("click", this.toggleNoCat);
    nocatItems.selectAll("span.epifilter-nocat-check")
       .classed("fa-check-square", function(d) {return d3.select(this.parentNode).datum().noTagSelected})
       .classed("fa-square", function(d) {return !d3.select(this.parentNode).datum().noTagSelected});

    //Adding category elements
    var itemUpd = canvas.selectAll("div.epifilter-item").data(d => d.items, d => d.name);
    var itemEnter = itemUpd.enter().append("div").classed("epifilter-item epifilter-elem", true);
    itemUpd.exit().remove();
    var items = itemUpd.merge(itemEnter); 
    items.sort().style("background-color", d => d.color);
    //adding item details
    itemEnter.append("span").classed("epifilter-item-sel epifilter-check", true).classed("far", true);
    itemEnter.append("span").classed("epifilter-item-name", true).text(d => d.name);
    itemEnter.on("click", this.toggleCat);
    items.selectAll("span.epifilter-item-sel")
             .classed("fa-check-square", function(d) {return d3.select(this.parentNode).datum().selected})
             .classed("fa-square", function(d) {return !d3.select(this.parentNode).datum().selected})
  };
  this.toggleAll = function(items) {
    var allSelected = items.filter(f => !f.selected).length==0; 
    if(allSelected)
      items.forEach(d => {d.selected = false;});
    else 
      items.forEach(d => {d.selected = true;});

    theFilter.render();
  };
  this.toggleNoCat = function(d) {
    d.noTagSelected = !d.noTagSelected
    theFilter.render();
  };
  this.toggleCat = function(d) {
    d.selected = !d.selected
    theFilter.render();
  };
  this.render();
}

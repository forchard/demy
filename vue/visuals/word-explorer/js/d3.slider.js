function epislider(selection, data) {
  this.selection = selection;
  this.data = data?[].concat(data):[];

  this.usedWidth = function(d) {return d.width - 2*d.padding};
  this.setScale = function(d) {d.scale = d.scale?d.scale:d3.scaleLinear().domain([d.min, d.max]).range([d.padding, d.width - d.padding]);return this};
  this.setAxis = function(d) {d.axis = d.axis?d.axis:d3.axisBottom().scale(d.scale).ticks(3);return this};
  this.draw = function() {
    //Making data and selection on the same size. If data not provided then set to default
    var defaultData = ({"min":0, "max":100, "from":10, "to":90, "width":300, "height":35, "padding":15, "cursorHeight":20, "cursorWidth":7, "axisHeight":17})
    this.setScale(defaultData).setAxis(defaultData);
    var selSize = this.selection.size();
    for(var i=this.data.length; i<selSize; i++) this.data.push(defaultData);
    this.data = this.data.slice(0, selSize);
    this.data.forEach(d => this.setScale(d).setAxis(d));

    //Adding a SVG group of class slider-canvas if missing on each selection element and attaching data to elements
    this.selection.data(this.data)
    this.selection.each(function(d) { 
      if(d3.select(this).selectAll(".slider-canvas").size() == 0 )
      {
        if(this.tagName === "SVG" || this.tagName === "G")
          d3.select(this).append("g").classed("slider-canvas", true).data(d)
        else { 
          d3.select(this).append("svg")
             .classed("d3-slider", true).classed("d3-slider-horizontal", true)
             .attr("width", d.width)
             .attr("height", d.height)
             .append("g").classed("slider-canvas", true).data(d)
        }
      }
      else
       d3.select(this).selectAll("slider-canvas").data(d);
    });
    //Base canvas selection
    var canvas = this.selection.selectAll("g.slider-canvas").data(this.data);
    //Adding axis to selection
    var axisUpdate = canvas.selectAll("g.d3-slider-axis").data(this.data);
    var axisEnter = axisUpdate.enter().append("g")
      .classed("d3-slider-axis", true)
    
    axisUpdate.merge(axisEnter)  
      .each(function(d) {d.axis(d3.select(this))})
      .attr("transform", d => `translate(0,${d.height - d.axisHeight})`)

    //Adding left slider bar
    var slideLeftUpdate = canvas.selectAll("rect.d3-slider-slide-left").data(this.data);
    var slideLeftEnter = slideLeftUpdate.enter().append("rect")
      .classed("d3-slider-slide-left", true)
      .attr("x",d => d.padding)
      .attr("y", d => d.height - d.axisHeight - d.cursorHeight*0.6)
      .attr("rx", 2).attr("ry", 2)
    slideLeftUpdate.merge(slideLeftEnter) 
     .attr("width", d => d.scale(d.from)-d.padding).attr("height", d => d.cursorHeight*0.6)

    //Adding middle slider bar
    var slideMiddleUpdate = canvas.selectAll("rect.d3-slider-slide-middle").data(this.data);
    var slideMiddleEnter = slideMiddleUpdate.enter().append("rect")
      .classed("d3-slider-slide-middle", true)
      .attr("y", d => d.height - d.axisHeight - d.cursorHeight*0.6)
      .attr("height", d => d.cursorHeight*0.6)
    slideMiddleUpdate.merge(slideMiddleEnter) 
      .attr("width", d => d.scale(d.to)-d.scale(d.from))
      .attr("x",d => d.scale(d.from))

    //Adding right slider bar
    var slideRightUpdate = canvas.selectAll("rect.d3-slider-slide-right").data(this.data);
    var slideRightEnter = slideRightUpdate.enter().append("rect")
      .classed("d3-slider-slide-right", true)
      .attr("y", d => d.height - d.axisHeight - d.cursorHeight*0.6)
      .attr("rx", 2).attr("ry", 2)
      .attr("height", d => d.cursorHeight*0.6)
    slideRightUpdate.merge(slideRightEnter) 
      .attr("width", d => d.scale(d.max)-d.scale(d.to))
      .attr("x",d => d.scale(d.to))

    //Adding from cursor & label
    var cursorFromUpdate = canvas.selectAll("g.d3-slider-cursor-from").data(this.data);
    var cursorFromEnter = cursorFromUpdate.enter().append("g")
      .classed("d3-slider-cursor-from", true)
    cursorFromEnter.append("rect")
      .classed("d3-slider-cursor", true)
      .attr("y", d => d.height - d.axisHeight - d.cursorHeight*0.8).attr("x", d => -d.cursorWidth/2.0)
      .attr("rx", 6).attr("ry", 2)
      .attr("width", d => d.cursorWidth).attr("height", d => d.cursorHeight)
      .call(d3.drag()
        .on("start", this.dragCursorStarted)
        .on("drag", this.draggedCursorFrom)
        .on("end", this.dragCursorEnded))
    cursorFromEnter.append("text")
      .classed("d3-slider-label", true)
      .attr("y", d => d.height)
    cursorFromUpdate.selectAll("rect.d3-slider-cursor").data(this.data)
    cursorFromUpdate.selectAll("text.d3-slider-label").data(this.data)

    cursorFromUpdate.merge(cursorFromEnter)
      .attr("transform", d => `translate(${d.scale(d.from)},0)`)
      .selectAll("text.d3-slider-label")
        .text(d => d.from) 
    ;

    //Adding to cursor & label
    var cursorToUpdate = canvas.selectAll("g.d3-slider-cursor-to").data(this.data);
    var cursorToEnter = cursorToUpdate.enter().append("g")
      .classed("d3-slider-cursor-to", true)
    cursorToEnter.append("rect")
      .classed("d3-slider-cursor", true)
      .attr("y", d => d.height - d.axisHeight - d.cursorHeight*0.8).attr("x", d => -d.cursorWidth/2.0)
      .attr("rx", 6).attr("ry", 2)
      .attr("width", d => d.cursorWidth).attr("height", d => d.cursorHeight)
      .call(d3.drag()
        .on("start", this.dragCursorStarted)
        .on("drag", this.draggedCursorTo)
        .on("end", this.dragCursorEnded));
    cursorToEnter.append("text")
      .classed("d3-slider-label", true)
      .attr("y", d => d.height)
      .text(d => d.to)
    cursorToUpdate.selectAll("rect.d3-slider-cursor").data(this.data)
    cursorToUpdate.selectAll("text.d3-slider-label").data(this.data)

    cursorToUpdate.merge(cursorToEnter)
      .attr("transform", d => `translate(${d.scale(d.to)},0)`)
      .selectAll("text.d3-slider-label")
        .text(d => d.to) 
    ;
  };
  this.dragCursorStarted = function(d) {
    d3.select(this).style("cursor", "grabbing");
  };
  this.dragCursorEnded = function(d) {
    d3.select(this).style("cursor", null);
  };
  this.draggedCursorFrom = function(d) {
      var x = d.scale(d.from) + d3.event.x;
      if(x<d.scale(d.min)) x = d.scale(d.min);
      if(x>d.scale(d.to)) x = d.scale(d.to);
      d3.select(this.parentNode)
        .attr("transform", d => `translate(${x},0)`)
        .select("text")
          .text(d.from|0)
      d.from = d.scale.invert(x);
      //Moving slide bars
      d3.select(this.parentNode.parentNode).selectAll("rect.d3-slider-slide-left")
        .attr("width", d => d.scale(d.from)-d.padding);
      d3.select(this.parentNode.parentNode).selectAll("rect.d3-slider-slide-middle")
        .attr("x",d => d.scale(d.from))
        .attr("width", d => d.scale(d.to)-d.scale(d.from));
  };
  this.draggedCursorTo = function(d) {
      var x = d.scale(d.to) + d3.event.x;
      if(x<d.scale(d.from)) x = d.scale(d.from);
      if(x>d.scale(d.max)) x = d.scale(d.max);
      d3.select(this.parentNode)
        .attr("transform", d => `translate(${x},0)`)
        .select("text")
          .text(d.to|0)
      d.to = d.scale.invert(x);
      //Moving slide bars
      d3.select(this.parentNode.parentNode).selectAll("rect.d3-slider-slide-right")
        .attr("x",d => d.scale(d.to))
        .attr("width", d => d.scale(d.max)-d.scale(d.to))
      d3.select(this.parentNode.parentNode).selectAll("rect.d3-slider-slide-middle")
        .attr("x",d => d.scale(d.from))
        .attr("width", d => d.scale(d.to)-d.scale(d.from));
  };
  this.draw();
}

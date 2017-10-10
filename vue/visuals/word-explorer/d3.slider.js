function epislider(selection) {
  this.selection = selection;
  this.value = 50
  this.width = 300
  this.cursorSize = 20
  this.padding = 5
  this.usedWidth = function() {return this.width - 2*this.padding}
  this.draw = function() {
    //Adding a SVG group of class slider-canvas
    var slider = this;
    this.selection.each(function() { 
      if(d3.select(this).selectAll("slider-canvas").size() == 0)
      {
        if(this.tagName === "SVG" || this.tagName === "G")
          d3.select(this).append("g").classed("slider-canvas", true)
        else { 
          d3.select(this).append("svg")
             .classed("d3-slider", true).classed("d3-slider-horizontal", true)
             .attr("width", slider.width)
             .append("g").classed("slider-canvas", true)
        }
      }
    })

    var axisScale = d3.scaleLinear().domain([10, 130]).range([0, this.usedWidth()]);
    var xAxis = d3.axisBottom().scale(axisScale);

    
    //<rect x="50" y="20" rx="20" ry="20" width="150" height="150" style="fill:red;stroke:black;stroke-width:5;opacity:0.5" />

    var canvas = this.selection.selectAll("g.slider-canvas")
    var axisUpdate = canvas.selectAll("g.slider-axis").data([1])
    var axisEnter = axisUpdate.enter().append("g")
      .classed("slider-axis", true)
      .call(xAxis)
      .attr("transform", `translate(${this.padding},20)`)

    var cursorUpdate = canvas.selectAll("rect.d3-slider-cursor").data([this.value])
    var cursorEnter = axisUpdate.enter().append("rect")
      .classed("d3-slider-cursor", true)
      .call(xAxis)
      .attr("y", 20 - this.cursorSize*0.8)
      .attr("rx", 2).attr("ry", 2)
      .attr("width", this.cursorSize).attr("height", this.cursorSize)
    cursorUpdate.merge(cursorEnter)
      .attr("x",this.padding+axisScale(this.value)-this.cursorSize/2.0)

    
  };
  this.draw();
}

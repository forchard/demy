var vRender = {};

vRender["Bars"] = {};
vRender["Scatter"] = {};
vRender["Filter"] = {};
vRender["Table"] = {};
vRender["Lines"] = {};

vRender["Scatter"].render = function(svg, data) {
  svg.html("")
// var data = d3.range(100).map(d3.randomBates(10));
var margin = {top: 10, right: 30, bottom: 30, left: 30},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var random = Math.random,
data = d3.range(50).map(function() { return [random() * width, random() * height]; });


var x = d3.scaleLinear()
    .domain([0, d3.max(data, function(d){ return d[0]; })])
    .range([0, width]);

var y = d3.scaleLinear()
    .domain([0, d3.max(data,function(d) { return d[1]; })])
    .range([height, 0]);

x.domain(d3.extent(data, function(d) { return d[0]; }))
y.domain(d3.extent(data, function(d) { return d[1]; }))

g.selectAll(".dot")
      .data(data)
    .enter().append("circle")
      .attr("class", "dot")
      .attr("r", 3)
      .attr("cx", function(d) { return x(d[0]); })
      .attr("cy", function(d) { return y(d[1]); })
      .style('fill', function(d) {
        return (x(d[0]) <= width/2 && y(d[1]) <= height/2) ? 'red' : 'blue';
      })


g.append("g")
        .attr("transform", "translate(0," + height + ")")
      .call(d3.axisBottom(x));
g.append("g")
      .call(d3.axisLeft(y));

}

vRender["Bars"].render = function(svg, data, properties) {
  svg.html("")

var data = d3.range(1000).map(d3.randomBates(10));

var formatCount = d3.format(",.0f");

var margin = {top: 10, right: 30, bottom: 30, left: 30},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var x = d3.scaleLinear()
    .rangeRound([0, width]);

var bins = d3.histogram()
    .domain(x.domain())
    .thresholds(x.ticks(20))
    (data);

var y = d3.scaleLinear()
    .domain([0, d3.max(bins, function(d) { return d.length; })])
    .range([height, 0]);

var bar = g.selectAll(".bar")
  .data(bins)
  .enter().append("g")
    .attr("class", "bar")
    .attr("transform", function(d) { return "translate(" + x(d.x0) + "," + y(d.length) + ")"; });

bar.append("rect")
    .attr("x", 1)
    .attr("width", x(bins[0].x1) - x(bins[0].x0) - 1)
    .attr("height", function(d) { return height - y(d.length); });

bar.append("text")
    .attr("dy", ".75em")
    .attr("y", 6)
    .attr("x", (x(bins[0].x1) - x(bins[0].x0)) / 2)
    .attr("text-anchor", "middle")
    .text(function(d) { return formatCount(d.length); });

g.append("g")
    .attr("class", "axis axis--x")
    .attr("transform", "translate(0," + height + ")")
    .call(d3.axisBottom(x));


}
// ///////////////////////////////////////////////////////Line chart
vRender["Lines"].render = function(svg, data, properties){
  svg.html("")

var data = [
  d3.range(20).map(d3.randomBates(10))
  ,d3.range(20).map(d3.randomBates(10))
]
var properties =
  {"series_colors":["#66CDAA", "#ff7500"]
      , "series_names":["serie 1", "serie 2"]
      , "xAxis_format":".2s"
      , "yAxis_format":""
      , "tooltip_format":[".4s",".4s"]
      , "show_legend":true
      , "show_labels":true
      , "labels_format":true
  }

var i = 0;
var graphData = data.map(d => {
  var ret = {"values":d, color:properties.series_colors[i]
  ,names:properties.series_names[i]
  ,xAxis_format:properties.xAxis_format[0]
  ,tooltip_format:properties.tooltip_format[i]}
  i++;
  return ret;
})
console.log(graphData)

var margin = {top: 10, right: 20, bottom: 20, left: 30},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    vis = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

// var random = Math.random,
//     data = d3.range(4).map(function() { return [random() * width, random() * height]; });

var max = d3.max(graphData,function(c){ return d3.max(c.values)})
var min = d3.min(graphData,function(c){ return d3.min(c.values)})
var namesLength = d3.max(graphData,function(d){ return d.names.length}) > 8 ? 8 : d3.max(graphData,function(d){ return d.names.length})


var x = d3.scaleLinear()
    .domain([0, graphData[0].values.length])
    .rangeRound([0, width > 100 ? width - namesLength*5 : width]);

var y = d3.scaleLinear()
    .domain([min,max])
    .range([height, 0]);

var valueline = d3.line()
    .x(function(d, i) { return x(i); })
    .y(function(d) { return y(d); })
    .curve(d3.curveCatmullRom.alpha(0.5));

var xAxis = d3.axisBottom(x)
    .ticks(NbTicksX(graphData,width),"s");

var yAxis = d3.axisLeft(y)
    .ticks(Math.round(height/50));

var line0 = d3.line()
    .x(function(d, i) { return x(i); })
    .y(function(d) { return y(d*0); })
    .curve(d3.curveCatmullRom.alpha(0.5));

graphData.forEach(d => {
  vis.append("path")
          .datum(d.values)
            .attr("d",line0)
            .attr("class", "line")
            .style('stroke',d.color)
            .transition()
            .duration(1000)
            .attr("d", valueline)
})

if (width > 100 && height>100){
  graphData.forEach((d,i) => {


    var legend = vis.append('g').attr('class','legend')
                    .attr('transform','translate(' + (width - namesLength*4) + ',' + i*15 + ')')

    legend.append('text').text(d.names)
      .style('fill', d.color)

    legend.append('circle')
      .attr('r',3)
      .attr('cx',-6)
      .attr('cy',-3)
      .style('fill', d.color)
    })
}


graphData.forEach((d,i) => {

var circles = vis.append('g')
        .attr('class','dot')
        .selectAll('.dot')
        .data(d.values)
        .enter().append('circle')
          .attr('class','dot')
          .attr('r', 3)
          .attr('cx', function(d, i) { return x(i); })
          .attr('cy', height)
          .style('fill',d.color);

    circles.transition()
        .duration(1000)
          .attr('cy', function(d) { return y(d); })

    circles.on("mouseover",function(d){
          var pos = this.getBoundingClientRect()
              , y = window.scrollY
              , x = window.scrollX
              , tooltip = d3.select('body').append('div')
                  .attr('class','tooltip')
                  
            tooltip.style('left', (pos.left + x)+"px")
            .style('top', (pos.top + y - 15)+"px")
            .text(d3.format(".4s")(d))


          })

      circles.on("mouseout",function(d){
            d3.selectAll('.tooltip')
              .transition()
              .duration(1000)
              .style('opacity',0.5)
              .remove()

        })


})

vis.append("g")
        .attr("transform", "translate(0," + height + ")")
      .call(xAxis);
vis.append("g")
      .call(yAxis);

function NbTicksX(data, width){
    var valMax = d3.format(".2s")(d3.max(data,function(d,i) { return i}));
    var valMin = d3.format(".2s")(d3.min(data,function(d,i) {return i}));
    var lengthValMax = valMax.toString().length;
    var lengthValMin = valMin.toString().length;
    var lengthMax;
    if (lengthValMin <= lengthValMax){
      lengthMax = lengthValMax + 1 ;
    }
    if (lengthValMin <= lengthValMax){
      lengthMax = lengthValMin + 1 ;
    }
    var tickSize = 6*lengthMax + 25 ;
    var nbTick = Math.floor(width / tickSize) ;
    return nbTick<1?1:nbTick ;
  }
}

vRender["Filter"].render = function(svg, data) {
  var data = ["Janvier", "Février", "Mars", "Avril", "Mai", "Juin", "Juillet", "Aout", "Septembre", "Octobre", "Novembre", "Decembre"];
  var margin = {top: 10, right: 10, bottom: 10, left: 10},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

   var addTo =g.selectAll("rect").data(data).enter()
   addTo.append('rect')
        .attr("class", "area").attr("clip-path", "url(#clip)")
        .attr('x', 0)
        .attr('y', function(d, i) {return i*(20+1)})
        .attr('width', width)
        .attr('height', 20)
        .style('fill', "#333333");
   addTo.append('text')
        .attr("class", "area").attr("clip-path", "url(#clip)")
        .attr('x', width/2)
        .attr('y', function(d, i) {return (i+1)*(20+1)-5})
        .attr('font-size', 13)
        .attr('fill', "#DDDDDD")
        .attr('alignment-baseline',"middle")
        .attr("text-anchor","middle")
        .text(function (d) {return d;});


}

vRender["Table"].render = function(svg, data) {
  var data = {"columnNames":["Nom", "Prenom", "Age"]
              ,"rows":[
                  ["Birrien", "Marie", "35"]
                  ,["Péron", "Camille", "35"]
                  ,["Bourdon", "Loïc", "20"]
                  ,["Ramirez", "Javiera", "56"]
                  ,["Riara", "Sev", "39"]
                  ,["Grenapin", "Clémence", "23"]
                  ,["Rieiro", "Maï", "37"]
               ]
               ,"widths":[0.4, 0.4, 0.2]
               ,"font-family":"Verdana"
               ,"font-size":10
               ,"background-color":"#FFFFFF"
               ,"background-color-inter":"#F3F3F3"
               }

  const margin = {top: 10, right: 10, bottom: 10, left: 10},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    rowHeight = data["font-size"]+2;
  var xCol = 0,
    i = 0;
  const colPositions = data.widths.map(w => r = {"from":xCol,"to":(xCol+=width*w), "index":i++})

  //Defs for column positions
  const uDef = svg.selectAll("defs").data([1])
  const eDef = uDef.enter().append("defs")
  uDef.exit().remove();

  //Paths in def for each column
  const uPaths = uDef.merge(eDef).selectAll("path").data(colPositions, d => d.index +"_"+d.left+"_"+d.width);
  const ePaths = uPaths.enter().append("path")
                       .attr("id", d => "colpos"+d.index)
                       .attr("d", d => "M "+d.from+" "+(data["font-size"]+1)+" H "+ d.to)
  uPaths.exit().remove();

  //G containers for each row
  const uRows = svg.selectAll("g").data([data.columnNames].concat(data.rows));
  const eRows = uRows.enter().append("g").attr("transform", (d, i) => "translate(" + margin.left + "," + (margin.top + (i+1)*rowHeight) + ")");
  uRows.exit().remove();
  const aRows = uRows.merge(eRows);
  //Text for each row
  eRows.append("text");
  const aRowText = aRows.selectAll("text")
    .attr("font-family", data["font-family"])
    .attr("font-size", data["font-size"])
  ;

  //Text path for each cell
  const uCell = aRowText.selectAll("textPath").data(d => d);
  const eCell = uCell.enter().append("textPath");
  uCell.exit().remove();
  const aCell = uCell.merge(eCell)
                 .attr("xlink:href", (d, i) => "#colpos"+i)
                 .text(d => d);
}

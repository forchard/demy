var vRender = {};

vRender["Bars"] = {};
vRender["Scatter"] = {};
vRender["Filter"] = {};
vRender["Table"] = {};
vRender["Lines"] = {};

vRender["Scatter"].render = function(svg, data) {
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




vRender["Bars"].render = function(svg, data) {
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
// Line chart
vRender["Lines"].render = function(svg, data){
var data = d3.range(20).map(d3.randomBates(10));


var margin = {top: 10, right: 30, bottom: 30, left: 40},
    width = +svg.attr("width") - margin.left - margin.right,
    height = +svg.attr("height") - margin.top - margin.bottom,
    g = svg.append("g").attr("transform", "translate(" + margin.left + "," + margin.top + ")");

var totalWidth = +svg.attr("width"); //test
var totalHeight = +svg.attr("height");//test

var x = d3.scaleLinear()
    .domain([0, d3.max(data,function(d,i) { return i*1000000; })])
    .rangeRound([0, width]);

var y = d3.scaleLinear()
    .domain(d3.extent(data,function(d) { return d; }))
    .range([height, 0]);

var line = d3.line()
    .x(function(d, i) { return x(i*1000000); })
    .y(function(d) { return y(d); })
    .curve(d3.curveCatmullRom.alpha(0.5));


var xAxis = d3.axisBottom(x)
    .ticks(NbTicksX(data,width),"s")

var yAxis = d3.axisLeft(y)
    .ticks(width/30)


g.append("path")
        .datum(data)
          .attr("class", "line")
          .attr("d", line);

g.selectAll('dot')
      .data(data)
      .enter().append('circle')
        .attr('r', 2)
        .attr('cx', function(d, i) { return x(i*1000000); })
        .attr('cy', function(d) { return y(d); });

g.append("g")
        .attr("transform", "translate(0," + height + ")")
      .call(xAxis);
g.append("g")
      .call(yAxis);

function NbTicksX(data, width){
  var valMax = d3.format(".2s")(d3.max(data,function(d,i) { return i*1000000}));
  var valMin = d3.format(".2s")(d3.min(data,function(d,i) {return i*10000}));
  var lengthValMax = valMax.toString().length;
  var lengthValMin = valMin.toString().length;
  var lengthMax;
  if (lengthValMin <= lengthValMax){
    lengthMax = lengthValMax + 1 ;
  }
  if (lengthValMin <= lengthValMax){
    lengthMax = lengthValMin + 1 ;
  }
  var tickSize = 6*lengthMax + 10 ;
  var nbTick = Math.floor(width / tickSize) ;
  console.log(nbTick)
  return nbTick ;
}
function NbTicksY(width){
  var tickSize = 6 + 10 ;
  var nbTick = Math.round(width / tickSize) ;
  console.log(nbTick)
  return nbTick ;
}

// format = d3.formatPrefix(",.0", 10e3)

// if valMax >= valMin

// getBBox return an objet with the text area
// text = g.selectAll('text')
// text.each(function() {
//       console.log(this.getBBox());
//     });
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

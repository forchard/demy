$(function(){
   setup();
});

var draggingField = false;
var draggingVisual = false;
var resizingVisual = false;
var removingField = false;
var resizeDown = false;
var resizeUp = false;
var resizeLeft = false;
var resizeRight = false;
var resizingData = null;
var currentVisualWidth=0;
var currentVisualHeight=0;
var defaultVisualWidth=2;
var defaultVisualHeight=2;
var lastVisualId = -1;
var resizerWeight = 4;
var addingToVisualId = null;
var keepOptionsOpen=false;
var currentVisualOptionIndex=-1;


function setup() {
 storage.getWorkspaces(chooseWorkspace);
}

function chooseWorkspace(err, workspaces) {
  if(err==null) {
    const ws = workspaces[0];
    storage.getWorkspace(ws, (err, data) => {
    if(err == null);
      applyWorkspace(data); 
    });
  }
}


function applyWorkspace(data) {
 model = data;
 setupFields();
 setupGrid();
}

function setupGrid() {
 d3.select("div.grid").selectAll("div").data(cells).enter()
   .append("div").attr("class", "cell")
   .on("mouseover", selectCell)
   .on("mouseout", unselectCell) 
 d3.select("div.pane-resizer").call(paneDraggable);
}

function setupFields() {
 calculateGroups();
 var groups = d3.select("div.fields").selectAll("div.table-group").data(model.groupedTables);
 //new groups
 var newGroups = groups.enter().append("div")
   .classed("table-group", true)
   .text(function(d) {return d.name;});
 //existing groups
 groups.text(function(d) {return d.name;});
 //removed groups
 groups.exit().remove();

 var tables = groups.merge(newGroups).selectAll("div.table").data(function(d) {return d.tables});
 //New tables
 var newTables = tables.enter().append("div")
   .attr("class", "table")
   .text(function(d) {return d.name;})
 ;
 //Expand button on new tables
 newTables.append("img")
   .classed("triangle-button", true)
   .attr("src", function(d) {if(d.collapsed) return "img/invertedTriangle.png"; else return "img/triangle.png";})
   .on("click", function(d, i, group) {
     var collapsed = d.collapsed;
     d3.select(group[i])
       .attr("src", function(d) {if(!d.collapsed) return "img/invertedTriangle.png"; else return "img/triangle.png";})
     d3.select(group[i].parentNode).selectAll("div.field").classed("fields-collapsed", !collapsed );
     d.collapsed = !collapsed;
    }); 

 //Existing Tables
 tables.text(function(d) {return d.name;});
 //Removed tables
 tables.exit().remove();

 var fields = tables.merge(newTables).selectAll("div.field").data(function(d) {return d.fields;})
 //New fields
 fields.enter()
   .append("div").attr("class", "field")
   .style("margin-left", function(d) {return (d.level*6)+"px";})
   .text(function(d) {return d.name;})
   .call(fieldDraggable)
   .on("mouseover", function(d, id, group){d3.select(group[id]).classed("focus", true)})
   .on("mouseout", function(d, id, group){d3.select(group[id]).classed("focus", false)})
   .classed("fields-collapsed", function(d, id, group) {return group[id].parentNode.__data__.collapsed})
 ;
 //Existing fields
 fields.text(function(d) {return d.name;})
 //Removed fields
 fields.exit().remove();
}

function calculateGroups() {
  model.groupedTables=[];
  var groups = {};
  for(var i = 0; i<model.tables.length; i++) {
    const group = model.tables[i].group;
    if(!groups[group]) {
      model.groupedTables.push({});
      groups[group] = model.groupedTables[model.groupedTables.length-1];
      groups[group].name = group;
      groups[group].tables = [];
    }
    groups[group].tables.push(model.tables[i]);
  }
}

function selectCell(dat, id, group) {
  if(draggingField) {
    var visualId = visualIndexAtPoint(d3.event.clientX, d3.event.clientY, "absolute");
    if(visualId>=0) {
      showOptionsAt(visualId);
    }
    else {
      d3.selectAll(boundaryCells(group, id, defaultVisualWidth, defaultVisualHeight)).classed("selected-cell", true);
    }
  }
  else if(draggingVisual) {
    d3.selectAll(boundaryCells(group, id, currentVisualWidth, currentVisualHeight)).classed("selected-cell", true);
  }
  else if(resizingVisual) {
    d3.selectAll(resizedCells(group, id, resizingData)).classed("selected-cell", "true");
  }
}

function unselectCell(dat, id, group) {
  d3.selectAll("div.selected-cell").classed("selected-cell dropped-cell", false);
}

function visualIndexAtPoint(x, y, type){
  if(type=="absolute") {
    var pBox = d3.select("div.grid").node().getBoundingClientRect();
    x = x - (pBox.x + window.scrollX);
    y = y - (pBox.y + window.scrollY);
  }
  for(var i=0;i<visuals.length;i++) {
    if(x >= visuals[i].x0 && x<=visuals[i].x1 && y>=visuals[i].y0 && y<= visuals[i].y1)
      return i;
  }
  return -1;
}
function boundaryCells(cells, index, width, height) {
  var nCols = 0;
  for(nCols = 0;nCols<cells.length && (nCols+1==cells.length || cells[nCols+1].getBoundingClientRect().left>cells[nCols].getBoundingClientRect().left);nCols++);
  nCols++;
  var ret = [];
  var ind=index;
  var i=0;
  while(ind<cells.length && i< width*height) {
    if(i % width <= ind % nCols) //avoid shape overflow canvas
      ret.push(cells[ind])
    if(Math.floor(i/width)<Math.floor((i+1)/width)) //jump to next line 
      ind = ind + nCols - width + 1;
    else
      ind++;
    i++;
  }
  return ret;
}
function resizedCells(cells, index, vData) {
  var nCols = 0;
  for(nCols = 0;nCols<cells.length && (nCols+1==cells.length || cells[nCols+1].getBoundingClientRect().left>cells[nCols].getBoundingClientRect().left);nCols++);
  nCols++;
  var ret = [];
  var coord = getCellCoordinates(nCols, index);
  var points = getVisualResizePoints(vData);
  if(resizeUp && coord.y <= points.down) points.up = coord.y; 
  if(resizeDown && coord.y >= points.up) points.down = coord.y; 
  if(resizeLeft && coord.x <= points.right) points.left = coord.x; 
  if(resizeRight && coord.x >= points.left) points.right = coord.x; 
  var width = points.right - points.left + 1; 
  var ind = points.up*nCols+points.left;
  var point = getCellCoordinates(nCols, ind);
  while(ind < cells.length && !(point.y>points.down)) {
    ret.push(cells[ind]);
    if(point.x == points.right) //jump to next line 
      ind = ind + nCols - width + 1;
    else
      ind++;
    point = getCellCoordinates(nCols, ind);
  }
  return ret;
}
function getCellCoordinates(nCols, index) {
  return {
    "x":(index % nCols)
    ,"y":(Math.floor(index/nCols))
  }
}

function getVisualResizePoints(vData) {
  return {
    "down":vData.posy+vData.height-1
    ,"up":vData.posy
    ,"right":vData.posx+vData.width-1
    ,"left":vData.posx
  }
}

function fieldDraggable(selection) {
  var body = d3.select("body");  
  selection.call(d3.drag()
    .container(body.node())
    .on("start", fieldDragStart)
    .on("drag",fieldDrag)
    .on("end", fieldDragEnd)
  );
}
function paneDraggable(selection) {
  var body = d3.select("body");
  selection.call(d3.drag()
    .container(body.node())
    .on("drag", resizePane)
  );
}
function visualDraggable(selection) {
  selection.call(d3.drag()
    .on("start", visualDragStart)
    .on("end", visualDragEnd)
  );
}
function visualResizable(selection) {
  selection.call(d3.drag()
    .on("start", visualResizeStart)
    .on("end", visualResizeEnd)
  );
}
function renderVisualData(visuals) {
  visuals.select("svg").remove()
  visuals.append("svg")
    .classed("visual-data", true)
    .attr("width", function(d) {return (d.x1-d.x0)}) 
    .attr("height", function(d) {return (d.y1-d.y0)}) 
    .each(function(d, i, g) {vRender[(d.renderAs && d.renderAs.name)?d.renderAs.name:visualGallery[0].name].render(d3.select(g[i])) })
}

function fieldDragStart() {
  draggingField = true;
  d3.select("div.grid").attr("style","z-index:4");
  var dragField = d3.select("#dragField")
  var clone = JSON.parse(JSON.stringify(d3.event.subject));
  dragField
    .text(clone.name)
    .style("top", (d3.event.y-5)+"px")
    .style("left", (d3.event.x-10)+"px")
    .style("display","block")
    .datum(clone)
  ;  
  d3.select("body").style("cursor", "grabbing");
  if(currentVisualOptionIndex>=0) {
    showOptionsAt(currentVisualOptionIndex);
  }
}
function visualDragStart() {
  hideOptions();
  draggingVisual = true;
  currentVisualWidth=d3.event.subject.width;
  currentVisualHeight=d3.event.subject.height;
  d3.select("div.grid").style("z-index","2");  
  d3.select("body").style("cursor", "grabbing");
} 
function resizePane() {
  d3.select("div.fields").style("width", d3.event.x+"px");
}
function visualResizeStart() {
  hideOptions();
  resizingVisual = true;
  setResize(d3.event.subject);
  resizingData = this.parentNode.__data__;

  d3.select("div.grid")
    .style("z-index","2")
    .style("cursor",d3.event.subject.direction);
}

function fieldDrag() {
  d3.select("#dragField")
    .style("top", (d3.event.y-5)+"px").style("left", (d3.event.x-10)+"px");
  setDropPosition(); 
}
function fieldDragEnd() {
  replaceSelectionByVisual();
  dropSelectedField();
  draggingField = false;
  d3.select("body").style("cursor", "auto");
  d3.select("#options").style("cursor", "auto");
  d3.select("#options").select("div.options-details").selectAll("div.options-detail-line").selectAll("div.value-placeholder").classed("accept-field", false);
}
function visualDragEnd() {
  draggingVisual = false;
  replaceSelectionByVisual(d3.event.subject)
  d3.select("body").style("cursor", "auto");
  d3.select("#dragField").datum(null);
}
function visualResizeEnd() {
  resizingVisual = false;
  replaceSelectionByVisual(this.parentNode.__data__);
}
function replaceSelectionByVisual(currentVisual) {
  d3.select("div.grid").style("z-index","0");
  d3.select("div.dragField").style("display","none");

  var selected = d3.selectAll("div.selected-cell").classed("dropped-cell",true).nodes();
  if(selected.length>0) {
    var nCols = 0;
    var cells =  d3.selectAll("div.cell").nodes();
    for(nCols = 0;nCols<cells.length && (nCols+1==cells.length || cells[nCols+1].getBoundingClientRect().left>cells[nCols].getBoundingClientRect().left);nCols++);
    nCols++;

    var x0=-1,x1=-1,y0=-1,y1=-1,posx,posy,minindex,maxindex;
    var pBox = d3.select("div.grid").node().getBoundingClientRect();
    for(var i=0;i<selected.length;i++) {
      var box = selected[i].getBoundingClientRect();
      var obj = d3.select(selected[i]).data()
      var left = box.left-pBox.left;
      var top = box.top-pBox.top;
      if(x0==-1 || x0>left) {x0=left;minindex=obj[0];}
      if(x1==-1 || x1<left+box.width) x1=left+box.width;
      if(y0==-1 || y0>top) y0=top;
      if(y1==-1 || y1<=top+box.height) {y1=top+box.height;maxindex=obj[0];}
    }

    renderVisual(currentVisual, x0, x1, y0, y1, minindex % nCols, Math.floor(minindex/nCols), (maxindex % nCols)-(minindex % nCols)+1,Math.floor(maxindex/nCols)-Math.floor(minindex/nCols)+1);
    var visualIndex;
    if(!currentVisual)
      visualIndex = lastVisualId;
    else
      visualIndex = currentVisual.id;
    showOptionsAt(visualIndex);
  }
}


function getVisualId(dat) {
  return dat.id;  
}

function renderVisual(currentVisual,x0, x1, y0, y1, posx, posy, width, height, title, fields) {
  //$("#console").text("x0:"+x0+", x1:"+x1+", y0:"+y0+", y1:"+y1+", posx:"+posx+", posy:"+posy+", width:"+width+", height:"+height+", title:"+title+", fields:"+fields)
  if(!currentVisual) {
    lastVisualId++;
    visuals.push({
      "id":lastVisualId,"x0":x0,"x1":x1,"y0":y0,"y1":y1,"posx":posx,"posy":posy,"width":width,"height":height,"title":title,"fields":fields, "update":false, "renderAs":null
    });
  }
  else {
    for(var i = 0; i<visuals.length;i++) {
      if(visuals[i].id == currentVisual.id) {
        visuals[i].update=true;
        visuals[i].x0=x0;
        visuals[i].x1=x1;
        visuals[i].y0=y0;
        visuals[i].y1=y1;
        visuals[i].posx=posx;
        visuals[i].posy=posy;
        visuals[i].height=height;
        visuals[i].width=width;
        visuals[i].title=title;
        visuals[i].fields=fields;
      }
      else {
        visuals[i].update = false;
      }
    }
  }
  renderVisuals();
}

function renderVisuals() {
  var existing = d3.select("div.canvas").selectAll("div.visual-container").data(visuals, getVisualId);
  var newVisuals = existing.enter();
  var deletedVisuals = existing.exit();
     
  //insert new
  newVisuals.append("div")
    .call(resizeVisuals)
    .classed("visual-container", true)
    .call(visualDraggable)
  
  //deleting removed
  deletedVisuals.remove();

  //updatingChanged
  existing.filter(function(d) {return d.update})
    .call(resizeVisuals); 

  for(var i = 0; i<visuals.length;i++) {
    visuals[i].update=true;
  }
}

function resizeVisuals(selection) {
  var visuals = selection
    .style("width", function(d) { return (d.x1-d.x0)+"px"; })
    .style("height", function(d) { return (d.y1-d.y0)+"px"; })
    .style("left", function(d) { return d.x0+"px"; })
    .style("top", function(d) { return d.y0+"px"; })
    .call(drawResizeLines)
    .call(renderVisualData)

}

function drawResizeLines(visuals) {
   existing = visuals.selectAll("div.resize").data(function(d) {return getResizers((d.x1-d.x0),(d.y1-d.y0))});

   existing.enter().append("div")
     .call(visualResizable)
     .merge(existing)
     .style("width", function(d) {return d.width;})
     .style("height", function(d) {return d.height;})
     .style("top", function(d) {return d.top;})
     .style("left", function(d) {return d.left;})
     .style("cursor", function(d) {return d.direction;})
     .classed("resize", true)
   ;
}
function getResizers(width, height) {
  
return  [
   {"direction":"w-resize","orientation":"vertical","height":(0.8*height)+"px","width":resizerWeight+"px","top":(0.1*height)+"px","left":"0px"}
  ,{"direction":"e-resize","orientation":"vertical","height":(0.8*height)+"px","width":resizerWeight+"px","top":(0.1*height)+"px","left":(width-resizerWeight)+"px"}
  ,{"direction":"n-resize","orientation":"horizontal","height":resizerWeight+"px","width":(0.8*height)+"px","top":"0px", "left":(0.1*width)+"px" }
  ,{"direction":"s-resize","orientation":"horizontal","height":resizerWeight+"px","width":(0.8*height)+"px","top":(height-resizerWeight)+"px","left":(0.1*width)+"px"}
  ,{"direction":"nw-resize","orientation":"horizontal","height":resizerWeight+"px","width":(0.1*height)+"px","top":"0px", "left":"0px"}
  ,{"direction":"nw-resize","orientation":"vertical","height":(0.1*height)+"px","width":resizerWeight+"px","top":"0px", "left":"0px"}
  ,{"direction":"sw-resize","orientation":"vertical","height":(0.1*height)+"px","width":resizerWeight+"px","top":(0.9*height)+"px", "left":"0px"}
  ,{"direction":"sw-resize","orientation":"horizontal","height":resizerWeight+"px","width":(0.1*height)+"px","top":(height-resizerWeight)+"px", "left":"0px"}
  ,{"direction":"se-resize","orientation":"vertical","height":(0.1*height)+"px","width":resizerWeight+"px","top":(0.9*height)+"px","left":(width-resizerWeight)+"px"}
  ,{"direction":"se-resize","orientation":"horizontal", "height":resizerWeight+"px","width":(0.1*height)+"px","top":(height-resizerWeight)+"px","left":(0.9*width)+"px"}
  ,{"direction":"ne-resize","orientation":"vertical", "height":(0.1*height)+"px", "width":resizerWeight+"px", "top":"0px", "left":(width-resizerWeight)+"px"}
  ,{"direction":"ne-resize","orientation":"horizontal", "height":resizerWeight+"px", "width":(0.1*height)+"px", "top":"0px", "left":(0.9*height)+"px"}
  ];

}

function setResize(d) {
  var dir = d.direction.replace("-resize", "");
  resizeUp = (dir.indexOf("n")>-1);
  resizeDown = (dir.indexOf("s")>-1);
  resizeLeft = (dir.indexOf("w")>-1);
  resizeRight = (dir.indexOf("e")>-1);
}

function showOptionsAt(visualIndex) {
  currentVisualOptionIndex=visualIndex;
  if(!visuals[visualIndex].renderAs)
    changeRender(visuals[visualIndex]);
  
  var pBox = d3.select("div.grid").node().getBoundingClientRect();
  var options = d3.select("#options");
  options.selectAll("input.hidden").data(["x"]).enter().append("input").classed("hidden", true).attr("type", "hidden")
    .call(function() { //this call is to ensure that this call will be done only the first time the options are created
      options.on("mouseover", function() { 
                                 showDropOptions();
                                 if(!options.datum())
                                   options.datum({"mouseover":true}); 
                                 options.datum().mouseover=true;
                                 }
             )
             .on("mouseout", function() {  options.datum().mouseover=false; })
      
    });

  options.select("div.options-title")
    .selectAll("div.close-button").data(["x"]).enter().append("div").classed("close-button", true)
    .on("click", hideOptions)
    .text(function(d) {return d;});
  var icons = options.select("div.pick-render").selectAll("div").data(visualGallery)
  var newIcons = icons.enter().append("div").append("img")
      .attr("src", function(d) {return "img/"+d.icon})
      .classed("visual-icon", true)
      .attr("alt", function(d) {return d.alt;})
      .attr("title", function(d) {return d.alt;})
      .on("mouseover", function(d) {d3.select(this).classed("focus2", true);})
      .on("mouseout", function(d) {d3.select(this).classed("focus2", false);})
      .on("click", function(d, i) {
       changeRender(visuals[currentVisualOptionIndex], visualGallery[i]); 
       showOptionsAt(currentVisualOptionIndex); 
      })
    ;

  icons.select("img").merge(newIcons)
    .classed("focus", function(d) {return d.name == visuals[visualIndex].renderAs.name;})

  var lines = options.select("div.options-details").selectAll("div.options-detail-line").data(visuals[visualIndex].renderAs.fields);
  //updating existing children datums
  lines.selectAll("div.options-detail").datum(function(d, i, group) { return group[i].parentNode.__data__ });
  //Creating new lines
  var newLines = lines.enter().append("div").classed("options-detail-line", true);
  newLines.append("div").classed("options-detail", true).classed("options-detail-name", true);
  var placeHolders = newLines.append("div").classed("options-detail", true).classed("value-placeholder", true)
         .on("mouseover", function(d) { d.mouseover = true;})
         .on("mouseout", function(d) { 
            var box = this.getBoundingClientRect();
            if(d3.event.clientX<= box.x   
               || d3.event.clientX>= box.x + box.width 
               || d3.event.clientY<= box.y  
               || d3.event.clientY>= box.y + box.height)
            {
              d.mouseover = false;
              removeSelectedFields();
            }
         })
  ;
  //Updating all lines (news + existing)
  var allLines = lines.merge(newLines);
  allLines.select("div.options-detail-name").text(function(d) {return d.name});

  var vals = allLines.select("div.value-placeholder").selectAll("div.field-value").data(function(d) {return d.values;});
 
  var news = vals.enter().append("div").classed("field-value", true)
    .on("mouseover", function(d, i, group) {
      if(!draggingField && d.type!="empty") { 
        var div = d3.select(group[i]).classed("focus", true)
        if(div.selectAll("div.close-button").size()==0) 
          div.insert("div", "div.function-choice").text("x").classed("close-button", true).on("click", function(d, i, group) {
	    d.type="selected";
            removingField = true;
            removeSelectedFields();
            removingField = false; 
          });
        if(div.selectAll("img.triangle-button").size()==0)
          div.insert("img", "div.function-choice").attr("src", "img/invertedTriangle.png").classed("triangle-button", true)
            .on("click", function(d, i, group) {
               renderFieldFunctions(d, group[i].parentNode.parentNode.__data__, group[i].parentNode.parentNode);
             }) 
      }
     })
    .on("mouseout", function(d, i, group) { 
      if(!draggingField) { 
        var box = this.getBoundingClientRect();
        if(d3.event.clientX<= box.x   
            || d3.event.clientX>= box.x + box.width
            || d3.event.clientY<= box.y  
            || d3.event.clientY>= box.y + box.height)
        { 
          var div = d3.select(group[i]).classed("focus", false)
          div.selectAll("div.close-button").remove(); 
          div.selectAll("img.triangle-button").remove();
        }
      }
     })
  ;
  var allvals = vals.merge(news)
     .classed("empty-field", function(d) {return d.type=="empty";})
     .classed("candidate-field-single", function(d, i, group) { return d.type=="selected" && group[i].parentNode.__data__.arity=="1"})
     .classed("candidate-field-multi", function(d, i, group) { return d.type=="selected" && group[i].parentNode.__data__.arity=="*"})
     .text(function(d) { return d.name; })
  ;
  vals.exit().remove();
  lines.exit().remove();

    

  options
   .style("left",((pBox.x + window.scrollX)+ visuals[visualIndex].x0)+"px")
   .style("top",(pBox.y+ window.scrollY + visuals[visualIndex].y1+1)+"px").style("display","flex")
   .style("width", (visuals[visualIndex].x1 - visuals[visualIndex].x0)+"px")
  ;
  
  

  if(draggingField) {
    options.style("cursor", "not-allowed"); 
  } else {
    options.style("cursor", "auto"); 
  }
}
function changeRender(visual, renderAs) {
    var first=false;
    if(!renderAs) {
      renderAs = visualGallery[0];
      first = true;
    }
    if(visual.renderAs && visual.renderAs.name == renderAs.name)
      return;

    renderAs = JSON.parse(JSON.stringify(renderAs));
    for(var i = 0;i<renderAs.fields.length;i++) {
      if(renderAs.fields[i].type == "axis" || renderAs.fields[i].type == "measure") {
        if(first) {
          renderAs.fields[i].values = [d3.select("#dragField").datum()];
          first=false;
        }
        else
          renderAs.fields[i].values = [{"type":"empty", "name":""}];
      }
      else
        renderAs.fields[i].values = []; 
    }
    renderAs.unusedFields = [];
    if(visual.renderAs) {
      oldRender = visual.renderAs
      if(oldRender.unusedFields) {
         for(var i=0; i<oldRender.unusedFields.length;i++) {
           oldRender.fields.push(oldRender.unusedFields[i]);
         }
      }
      for(var i=0; i<oldRender.fields.length; i++) {
        //Try to find a field with the same name
        var old = oldRender.fields[i];
        for(j=0;j<renderAs.fields.length;j++) {
          if(renderAs.fields[j].name == old.name && renderAs.fields[j].type == old.type && old.values && old.values.length>0) {
            while(old.values.length>0 && !(renderAs.fields[j].arity=="1" && renderAs.fields[j].values && renderAs.fields[j].values.length>1 )) {
              if(!renderAs.values) { renderAs.values = []; }
              renderAs.values.push(old.values[0]);
              old.values.splice(0, 1);
            }
            if(old.values.length>0) {
              renderAs.unusedFields.push(old);
            }
          } 
        } 
      }
    } 
    visual.renderAs = renderAs;
    d3.selectAll("div.visual-container").filter(function(d) {return d.id == visual.id}).call(renderVisualData); 
}

function hideOptions() {
  d3.select("#options").style("display", "none");
  currentVisualOptionIndex = -1;
}

function showDropOptions() {
  if(draggingField) {
    var field = d3.select("#dragField").datum();
    var places = d3.select("#options").select("div.options-details").selectAll("div.options-detail-line").selectAll("div.value-placeholder");
    places.classed("accept-field",  function(d) {
                                      d.acceptingField =  (d.type == "measure" || (d.type == "axis" && field.type == "column"))
                                      return d.acceptingField;  
                                    })
    ;
  }
}

function setDropPosition() {
  var field = d3.select("#dragField").datum();
  var options=d3.select("#options");
  if(field && options.datum() && options.datum().mouseover) {
    var placeholder = options.select("div.options-details").selectAll("div.options-detail-line").selectAll("div.value-placeholder")
                              .filter(function(d) {return d.acceptingField && d.mouseover} )

    if(placeholder.size()>0) {
      //If we are here we are dragging over palce holder that accepts the current dragging field
      var vals = visuals[currentVisualOptionIndex].renderAs.fields.filter(function(v) {return v.name == placeholder.datum().name})[0].values;
      //if there is an empty field we replace it by the current field
      var inserted=false;
      for(var i=0;i<vals.length;i++) {
        if(vals[i].type=="empty") {
          vals[i].type="selected";
          vals[i].candidate = field;
          inserted = true;
          break;
        }  
      }
      if(!inserted) {
        var mouseY = d3.event.y;
        //Counting the number of divs after the cursor
        var elementsBefore = placeholder.selectAll("div.field-value").filter(function(d, i, group) {
          var box = group[i].getBoundingClientRect();
          return d.type!="selected" &&  (box.y + box.height + window.scrollY)<=mouseY; 
        });
        var oldIndex = -1;
        for(var i=0;i<vals.length;i++) {
          if(vals[i].type=="selected") {
            oldIndex=i;
            break;
          }
        }
        var newIndex = elementsBefore.size();
        if(oldIndex != newIndex) {
           //The position has changed
           //we remove current selected position if exists
           for(var i=0;i<vals.length;i++) {
             if(vals[i].type=="selected") {
               vals.splice(i, 1);
               break;
             }
           }
           vals.splice(newIndex, 0, {"name":"", "type":"selected","candidate":field});
           inserted = true;
        }
      }
      if(inserted) {
        showOptionsAt(currentVisualOptionIndex);
      }
    }
  }
}
function removeSelectedFields() {
  var changes = false;
  if(draggingField || removingField) {
    var fields = visuals[currentVisualOptionIndex].renderAs.fields;
    for(var i=0;i<fields.length;i++) {
      for(var j=0; j<fields[i].values.length;j++) {
        if(fields[i].values.length ==1 && fields[i].values[j].type=="selected") {
          fields[i].values[j].type="empty";
          fields[i].values[j].name="";
          fields[i].values[j].candidate=null;
          changes = true;
        } else if(fields[i].values[j].type=="selected"){
          fields[i].values.splice(j, 1);
          changes = true;
          j--;
        }
      }
    }
  }
  if(changes) {
    showOptionsAt(currentVisualOptionIndex);
  }
}
function dropSelectedField() {
  var changes = false;
  if(draggingField) {
    var fields = visuals[currentVisualOptionIndex].renderAs.fields;
    for(var i=0;i<fields.length;i++) {
      for(var j=0; j<fields[i].values.length;j++) {
        if(fields[i].values[j].type=="selected" && fields[i].arity=="*"){
          fields[i].values[j]=fields[i].values[j].candidate;
          changes = true;
        }
        else if (fields[i].values[j].type=="selected" && fields[i].arity=="1"){
          fields[i].values = [fields[i].values[j].candidate];
          changes = true;
        }
      }
    }
  } 
  if(changes)
    showOptionsAt(currentVisualOptionIndex);
}

function renderFieldFunctions(fieldData, holderData, valueItem) {
  var holder = d3.select(valueItem);
  var options = null;
  if(holderData.type == "axis") {
    options = transformationFunctions 
  }
  else if(holderData.type == "measure") {
    options = measureFunctions 
  }
  //Set the first option if not selected
  if(holderData.type == "axis" && !fieldData.transformation) {
    fieldData.transformation = options[0].name;
    currentChoice = options[0].name;
  }
  else if(holderData.type == "axis") {
    currentChoice = fieldData.transformation;
  }
  else if(holderData.type == "measure" && !fieldData.aggregation) {
    fieldData.aggregation = options[0].name;
    currentChoice = options[0].name;
  }
  else if(holderData.type == "measure") {
    currentChoice = fieldData.aggregation;  
  }

  //Let's check if the "other" option is to be selected
  if(currentChoice && options.filter(function(v) {return v.name == currentChoice}).length==0) {
      options.filter(function(v) {return v.isOther})[0].name = currentChoice;
  }
  //Creating the selection options
  var oData = JSON.parse(JSON.stringify(options));
  var choices = holder.selectAll("div.field-value").selectAll("div.function-choice").data(oData);
  var newChoices = choices.enter().append("div").classed("function-choice", true)
     .each(function(d, i, g) { 
         if(!g[i].parentNode.__data__.randomName)
           g[i].parentNode.__data__.randomName = "N"+Math.floor(Math.random()*1000000000);
         d.randomName = g[i].parentNode.__data__.randomName;
      })
  newChoices.append("input").attr("type", "radio").attr("name", function(d, i) {return d.randomName; });
  newChoices.append("span")

  var allChoices = choices.merge(newChoices);
  allChoices.selectAll("input")
    .property("checked", function(d) {return d.name == currentChoice})
    .style("display", fieldData.showDetails);
  allChoices.selectAll("span").text(function(d) {return d.name});
  choices.exit().remove();
}

function out(text) {
  d3.select("#console").text(text);
}

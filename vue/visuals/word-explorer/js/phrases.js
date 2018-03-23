function PhraseGrid(selector, indexPath, phrasesPath, tagsByClusters, tags) {
  var thisGrid = this;

  this.selector = selector;
  this.indexPath = indexPath;
  this.phrasesPath = phrasesPath;
  this.tagsByClusters = tagsByClusters;
  this.tags = tags;

  this.index;
  this.clusterId;
  this.seeBeforePhrases = [];
  this.seeAfterPhrases = [];

  this.init = function(clusterId, callback) {
    d3.json(this.indexPath, function(error, index) {
      if(error){
        console.warn(error)
        if(typeof callback != "undefined") callback(null, "Cannot load index");
      } else {
           thisGrid.initTable();
           thisGrid.index = index;
           thisGrid.clusterId = clusterId;
           if(typeof clusterId != "undefined" && clusterId != null) thisGrid.loadPage(clusterId, 0, callback);
           else if(typeof callback != "undefined") callback(context, null);
        }
      })
    };

   this.initTable = function() {
     //Pagination
     var pUpdate = d3.select(this.selector).selectAll("div.phrase-pages").data([1]);
     var pEnter = pUpdate.enter().append("div").classed("phrase-pages", true);
     var pages = pUpdate.merge(pEnter);

     //Table
     var tUpdate = d3.select(this.selector).selectAll("table.phrase-table").data([1]);
     var tEnter = tUpdate.enter().append("table").classed("phrase-table", true);
     var table = tUpdate.merge(tEnter);
     var hRowUpdate = table.selectAll("tr.phrase-header").data([1]);
     var hRowEnter = hRowUpdate.enter().append("tr").classed("phrase-header", true);
     var hRow = hRowUpdate.merge(hRowEnter);
     var hUpdate = hRow.selectAll("td.phrase-header").data(["N°", "Tags",  "Phrase", "%match"]);
     var hEnter = hUpdate.enter().append("th").classed("phrase-header", true).text(d => d);
     var header = hUpdate.merge(hEnter);
     
   };

   this.loadPage = function(clusterId, page ,callback) {
     
     if(this.index[clusterId] && this.index[clusterId][page]) {
       this.clusterId = clusterId;
       this.seeBeforePhrases = [];
       this.seeAfterPhrases = [];
       //Pagination
       this.loadPagination(Object.keys(this.index[clusterId]).map(i => (parseInt(i)==page)?true:false));
       //Table
       d3.json(this.phrasesPath + "/" + this.index[clusterId][page] , function(error, phrases) {
         if(error){
           console.warn(error)
           if(typeof callback != "undefined") callback(null, "Cannot load phrases");
         } else {
              thisGrid.tagPhrases(phrases);
              thisGrid.loadPhrases(phrases);
              if(typeof callback != "undefined") callback(phrases, null);
           }
         });
        } else {
          if(typeof callback!="undefined") 
            callback(null, "Cluster not found");
          else console.warn("Cannot found cluster to draw phrases")
        }
     };
   this.loadPhrases = function(phrases) {
     var trUpdate = d3.select(this.selector).select("table.phrase-table").selectAll("tr.phrase").data(phrases)
     var trEnter = trUpdate.enter().append("tr").classed("phrase", true);
     var trExit = trUpdate.exit().remove();
     var trs = trUpdate.merge(trEnter);

     trEnter.append("td").classed("phrase-nb phrase-cell", true);
     trEnter.append("td").classed("phrase-tags phrase-cell", true);
     var tdTextEnter = trEnter.append("td").classed("phrase-text phrase-cell", true);
     tdTextEnter.append("span").classed("phrase-text-before", true);
     tdTextEnter.append("span").classed("phrase-text-current", true);
     tdTextEnter.append("span").classed("phrase-text-after", true);
     trEnter.append("td").classed("phrase-match phrase-cell", true);

     trs.select("td.phrase-nb").text((d, i) => i);
     var tagsUpd = trs.select("td.phrase-tags").selectAll("span.tag").data(d => Object.keys(d.tags).map(t => d.tags[t]));
     trs.select("td.phrase-text").select("span.phrase-text-before")
        .text("...").style("display", d => Boolean(d.before)?"inline":"none").on("click", this.toggleBeforePhrase);
     trs.select("td.phrase-text").select("span.phrase-text-current").text(d => d.phrase);
     trs.select("td.phrase-text").select("span.phrase-text-after")
        .text("...").style("display", d => Boolean(d.after)?"inline":"none").on("click", this.toggleAfterPhrase);
     trs.select("td.phrase-match").text(d => (d.similarity*100).toFixed(2)+"%");

     var tagsEnt = tagsUpd.enter().append("span").classed("tag", true).classed("tag fas fa-square", true).style("margin","1px");
     tagsUpd.exit().remove();
     var allTags = tagsUpd.merge(tagsEnt)
               .style("color", d => [d.color].map(c => `rgb(${c[0]},${c[1]},${c[2]})`)[0]); 

   };
   this.loadPagination = function(pages) {
     var pUpdate = d3.select(this.selector).select("div.phrase-pages").selectAll("span.phrase-page-container").data(pages, (d,i) => i+":"+d);
     var pEnter = pUpdate.enter().append("span").classed("phrase-page-container", true);
     pEnter.filter((d, i) => i>0).append("span").classed("phrase-page-sep", true).text(",")
     pEnter.append("span").classed("phrase-page", true)
                        .classed("phrase-page-selected", d => d)
                        .on("click", this.changePagination)
                        .html((d, i) => (i<10)?"&nbsp;"+i:i);

     var pExit = pUpdate.exit().remove();

     var containers = pUpdate.merge(pEnter).order();
     var fixed = 1
     var rolling = 10
     var selected = pages.indexOf(true);
     var limit = (selected < rolling)?rolling:((selected+rolling > pages.length -1)?(pages.length-1 - rolling):selected);

     containers.select("span.phrase-page")
                        .style("display", (d, i) => (i<fixed || Math.abs(i - limit)<rolling || i+fixed > pages.length-1)?"inline":"none") 
     containers.select("span.phrase-page-sep")
                        .style("display", (d, i) => (i<fixed || Math.abs(i - limit)<rolling || i+fixed > pages.length-1)?"inline":"none") 
                        .text((d, i) => (i-1 <fixed || i-1 + fixed > pages.length - 1 || Math.abs(i-1 - limit)<rolling)?",":"...")  
   };
   this.changePagination = function(d, i, g) {
     if(!d) {
       thisGrid.loadPage(thisGrid.clusterId, i);
     }
   };
   this.toggleBeforePhrase = function(d, i, g) {
     var target = d3.select(this);
     if(target.text()==="...") target.text(d.before);
     else target.text("...");
   };
   this.toggleAfterPhrase = function(d, i, g) {
     var target = d3.select(this);
     if(target.text()==="...") target.text(d.after);
     else target.text("...");
   };
   this.tagPhrases = function(phrases) {
     var i = 0;
     var tagHierarchy = this.getTagHierarchy();
     phrases.map(phrase => {
       phrase.tags = {};
       var current = tagHierarchy; 
       var stop = false;
       var steps = phrase.hierarchyId.split(",");
       var i = 0;
       while(i < steps.length && !stop) {
         if(typeof current[steps[i]] == "undefined") stop = true;
         else {
           if(typeof current["tags"] != "undefined") {
             current["tags"].forEach(tag => phrase.tags[tag] = this.tags[tag]);
           }
           current = current[steps[i]];
           i = i + 1;
         }
       }
     });
   };
/*   this.clusterOrder = function(a, b) {
     var i = 0, aStart = 0, aEnd = a.indexOf(","), bStart = 0, bEnd = b.indexOf(",");
     while(true) {
       if((aEnd = -1 || aEnd - aStart < 1) && (bEnd = -1 || bEnd - bStart < 1)) return 0;
       if(aEnd = -1 || aEnd - aStart < 1) return -1;
       if(bEnd = -1 || bEnd - bStart < 1) return 1;
       var aVal = parseInt(a.substring(aStart, aEnd);
       var bVal = parseInt(b.substring(bStart, bEnd);
       if(aVal<bVal) return -1;
       if(bVal<aVal) return 1;
       aStart = aEnd+1;
       bStart = bStart+1;
       aEnd = a.indexOf(",", aStart);
       bEnd = b.indexOf(",", bStart);
     }
   };*/
   this.getTagHierarchy = function() {
     var ret = {};
     Object.keys(this.tagsByClusters).forEach(taggedCluster => {
       var current = ret;
       path = "";
       taggedCluster.split(",").forEach(p => {
         if(typeof current[p] === "undefined") current[p] = {};
         path = path + (path.length==0?"":",")+p;
         if(path === taggedCluster) current["tags"]=tagsByClusters[taggedCluster];
         current = current[p];
       });
     });
     return ret;
   }
   
}


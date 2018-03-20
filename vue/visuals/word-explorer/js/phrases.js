function phraseGrid(selector, indexPath, phrasesPath) {
  this.selector = selector;
  this.index;
  this.indexPath = indexPath;
  this.phrasesPath = phrasesPath;

  this.init = function(focusCluster, callback) {
    d3.json(this.indexPath, function(error, index) {
      if(error){
        console.warn(error)
        if(typeof callback != "undefined") callback(null, "Cannot load index");
      } else {
           this.initTable();
           this.index = index;
           if(typeof focusCluster != "undefined") this.loadCluster(focusCluster)
           if(typeof callback != "undefined") callback(context, null);
        }
      }
    };

   this.initTable = function() {
     var tUpdate = d3.select(this.selector).selectAll("table.phrase-table").data([1]);
     var tEnter = tUpdate.enter().append("table").classed("phrase-table", true);
     var table = tUpdate.merge(tEnter);
     var hRowUpdate = table.selectAll("tr.phrase-header").data([1]);
     var hRowEnter = hRowUpdate.enter().append("tr").classed("phrase-header", true);
     var hRow = hRowUpdate.merge(hRowEnter);
     var hUpdate = hRow.selectAll("td.phrase-header").data(["Phrase"]);
     var hEnter = hUpdate.enter().append("th").classed("phrase-header", true).text(d => d);
     var header = hUpdate.merge(hEnter);
     
   }
   this.loadCluster = function(clusterId, page, callback) {
     if(this.index[clusterId] && this.index[clusterId][page]) {
       d3.json(this.phrasesPath + "/" + this.index[clusterId][page] , function(error, phrases) {
         if(error){
           console.warn(error)
           if(typeof callback != "undefined") callback(null, "Cannot load phrases");
         } else {
              this.loadPhrases(phrases);
              if(typeof callback != "undefined") callback(phrases, null);
           }
         }
          
        } else {
          callback(null, "Cluster bot found");
        }
     };
   this.loadPhrases(phrases) {
     var trUpdate = d3.select(this.selector).select("table.phrase-table").selectAll("tr.phrase").data(phrases)
     var trEnter = trUpdate.enter().append("tr").classed("phrase-table", true);
     var trExit = trUpdate.exit().remove();
     var trs = trUpdate.merge(trEnter).sort()
   }

}


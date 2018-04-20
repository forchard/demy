var query = function() {
  var thisQ =  this;
  this.axis = {};
  this.measures = {};
  this.fields = {}
  this.tables = {}
  this.addField = function(field) {
     //Adding table if not present
     if(!Boolean(this.tables[field.table])) {
       this.tables[field.table] = field.table
     }
     //Adding field
     if(!Boolean(this.fields[this.fieldId(field)])) {
       this.fields[this.fieldId(field)] = field
     }
     //Adding fields as measures or axis
     if(!Boolean(field.aggregation)) {
       //Field is goin to be considered as an axis (GROUP BY)
       if(!Boolean(this.axis[this.fieldId(field)])) {
         this.axis[this.fieldId(field)] = field
       }
     }
     else {
       //Field is going to be considered as a measure (SUM, COUNT, etc)
       if(!Boolean(this.measures[this.fieldId(field)])) {
         this.measures[this.fieldId(field)] = field
       }
       
     }
  }
  this.addFields = function(fields) {
    fields.forEach(f => this.addField(f));
  }
  this.fieldId = function(field) {
    return field.table + "[" +field.name + "]";
  }
  this.rawData = function() {
    //TODO: Implement getting data from json stored files on model
    var ret = [];
    for(var i=0;i<1000;i++) {
      var line = {}
      Object.keys(this.fields).forEach(colKey => line[colKey] = Math.floor(Math.random()*20))
      ret.push(line);
    }
    return ret;
  }

  this.data = function() {
    var grouped = {}
    var raw = this.rawData()
    //grouping
    raw.forEach(line => {
      var key = this.getGroupKey(line)
      if(!Boolean(grouped[key]))
        grouped[key] = [];
      line["__count__"] = 1; //This is for allowing counting
      grouped[key].push(line);
    })
    //reducing
    var reduced = Object.keys(grouped)
      .map(key => grouped[key].reduce(this.reduceLines))
      .map(groupedLine => {
        Object.keys(this.measures).forEach(columnKey => groupedLine[columnKey] = [].concat[groupedLine[columnKey]][0])
        return groupedLine
      });
    
    return reduced; 
  }

  this.getGroupKey = function(line) {
    if(Object.keys(this.axis).length == 0) return "~~";
    return Object.keys(this.axis).map(k => line[k]).join("~~")
  }

  this.reduceLines = function(l1, l2) {
    var line = {"__count__":l1["__count__"]+l2["__count__"] }
    Object.keys(thisQ.axis).forEach(colKey => {line[colKey] = l1[colKey]})
    Object.keys(thisQ.measures).forEach(colKey => {
      switch(measures[colKey].aggregation.toLowerCase()) {
        case "sum": line[colKey] = l1[colKey] + l2[colKey];break;
        case "count": line[colKey] = line["__count__"];break;
        case "average": line[colKey] = (l1[colKey] + l2[colKey])/line["__count__"];break;
        default: line[colKey] = line["__count__"];break;
      }
    });
    return line;
  }
  
}

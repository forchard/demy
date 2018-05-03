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
        let id = this.fieldId(field).split('[')
        id.shift()
        id = id.toString()
        id = id.substr(0, id.length-1)
        this.axis[id] = field
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
  // this.addFieldsName = function(fieldsName){
  //   fields.
  // }
  this.fieldId = function(field) {
    // return field.name
    return field.table + "[" +field.name + "]";
  }
  this.rawData = function(fields) {
    return new Promise((resolve,reject)=>{
    //todo: Implement getting data from json stored files on model
    const wks = d3.select("span.workspace-name").text()
    const aData = []
    let url = ""
      Object.keys(this.fields).forEach((d,i)=>{
        url += '/'+ d
      })
     storage.getData(url,wks).then((d)=>{
          resolve(d)
      })
  })
}
  this.data = function() {
    var grouped = {}
    var raw = this.rawData()
    //grouping
    return raw.then((d)=>{
      d.forEach(line => {
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
    })
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

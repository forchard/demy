exports.sendData = function(path){
  return new Promise((resolve,reject)=>{
    const fs = require('fs')
    , ws = path.shift()
    , aRaws = path

    const tableName = aRaws[0].split("[")[0]
    aRet = []

    fs.readFile(`../data/fod/workspaces/${ws}/dataset/${tableName}.json`, 'utf8', function (err,data) {
      if (err) {
        return console.log(err);
      }
      const aData = JSON.parse(data)
      const aRet = aData.map(line=>{
        let newLine = {}
        aRaws.forEach(colName =>{
          let id = colName.split("[")
          id.shift()
          let sId = id.toString()
          sId = sId.substr(0, sId.length -1)
          newLine[sId] = line[sId]
        })
        return newLine
      })
      const retJson = JSON.stringify(aRet)
      resolve(retJson)
    })
  })
}

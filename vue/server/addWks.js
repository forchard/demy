const fs = require('fs')
const model = require('./model')

function addWks(name,login,psw,url) {
  let path = '../data/fod/' + name.toUpperCase()
  console.log(path)
  if (!fs.existsSync(path)){
    fs.mkdirSync(path ,(err) => {
      if (err) throw err;
      console.log('The folder workspace has been saved!');
    })
    oDatasources = {
      'login':login
      ,'psw':psw
      ,'url':url
    }
    jsonDatasource = JSON.stringify(oDatasources)
    path = path + 'dataset.json'
    console.log(path)
    fs.writeFileSync(path + 'dataset.json', jsonDatasource, (err) => {
      if (err) throw err;
      console.log('The dataset.json has been saved!');
    })
  }

}
addWks('test1','test','test')

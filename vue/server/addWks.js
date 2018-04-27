const fs = require('fs')
const model = require('./model')

 exports.createWks = function(name,login,psw,url) {
  return new Promise((resolve1,reject1) => {

    return new Promise((resolve,reject)=>{
      let path = '../data/fod/workspaces/' + name
      console.log(path)
      if (!fs.exists(path)){
        fs.mkdir(path ,(err) => {
          if (err) throw err;
          console.log('The folder workspace has been saved!');
          resolve(path)
        })
      }else{
        console.log('file already exist')
      }
    }).then((path) => {
      oDatasources = [{
        'login':login
        ,'psw':psw
        ,'url':url
      }]
      jsonDatasource = JSON.stringify(oDatasources)
      fs.writeFile(path + '/datasources.json', jsonDatasource, (err) => {
        if (err) throw err;
        console.log('The dataset.json has been saved!');
        resolve1(path)
      })
      fs.mkdir(path +'/dataset', function(err){
        if (err) console.log(err)
      })
    })
    .catch((err)=>{
      if(err) throw err
    })
  })
  .catch((err)=>{
    if(err) throw err
  })

}

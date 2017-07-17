var fs = require('fs')
    ,conf = require('./conf')

function getFile(dirs, filename, callback) {
 if(!dirs || !(dirs.constructor === Array))
   return callback(new Error("dirs must be an array's string"));

 var getTheFile = (path, filename, callback) => {
   if(!soundFileName(filename))
     return callback(new Error(filename+": User name or model names cannot contain /"))
   path = path+"/"+filename
   fs.open(path, "r", (err, fd) => {
   if(err && err.code ==="ENOENT")  
     return callback(null,null);
   if(err) return callback(err);
   return callback(null, fs.createReadStream(null, {"fd":fd})); 
   });
 };
 var checkPart = (dirs, i, path, filename, callback) => {
   if(i==dirs.length)
     return getTheFile(path, filename, callback);
   else {
     var dir = dirs[i];
     path = path + "/" + dir;
     if(!soundFileName(dir))
       return callback(new Error(dir+": User name or model names cannot contain /"))
     fs.mkdir(path, "0774", (err) => {
     if(err && err.code != "EEXIST") return callback(err);
     return checkPart(dirs,i+1, path, filename, callback);
     }); 
   }    
 }
 var i = 0;
 var path = conf.workspace.path;
 return checkPart(dirs, i, path, filename, callback);
}

function saveFile(content, dirs, filename, callback) {
 if(!dirs || !(dirs.constructor === Array))
   return callback(new Error("dirs must be an array's string"));

 var saveTheFile = (content, path, filename) => {
   if(!soundFileName(filename))
     return callback(new Error("User name or model names cannot contain /"))
   path = path+"/"+filename
   fs.open(path, "w", (err, ws) => {
   if(err) return callback(err);
   fs.write(ws, content, (err) => {
   if(err) return  callback(err);
   return callback(null); 
   })});
 };
 var ckeckPart = (content, dirs, i, path, filename) => {
   if(i==dirs.length)
     return saveTheFile(content, path, filename);
   else {
     var dir = dirs[i];
     path = path + "/" + dir;
     if(!soundFileName(dir))
       return callback(new Error("User name or model names cannot contain /"))
     fs.mkdir(path, "0774", (err) => {
     if(err && err.code != "EEXIST") return callback(err);
     return checkPart(content, dirs,i+1, path, filename);
     }); 
   }    
 }
 var i = 0;
 var path = conf.workspace.path;
 return checkPart(content, dirs, i, path, filename);
}

function listFiles(dirs, callback) {
 if(!dirs || !(dirs.constructor === Array))
   return callback(new Error("dirs must be an array's string"));

 var getTheFiles = (path, callback) => {
   fs.readdir(path, (err, files) => {
   if(err && err.code ==="ENOENT")  
     return callback(null,null);
   if(err) return callback(err);
   return callback(null, files); 
   });
 };
 var checkPart = (dirs, i, path, callback) => {
   if(i==dirs.length)
     return getTheFiles(path, callback);
   else {
     var dir = dirs[i];
     path = path + "/" + dir;
     if(!soundFileName(dir))
       return callback(new Error(dir+": User name or model names cannot contain /"))
     fs.mkdir(path, "0774", (err) => {
     if(err && err.code != "EEXIST") return callback(err);
     return checkPart(dirs,i+1, path,  callback);
     }); 
   }    
 }
 var i = 0;
 var path = conf.workspace.path;
 return checkPart(dirs, i, path, callback);
}


function soundFileName(name) {
  return name && name.length>0 && name.indexOf("/")<0;
}

exports.soundFileName = soundFileName;
exports.getFile = getFile;
exports.saveFile = saveFile;
exports.listFiles = listFiles;


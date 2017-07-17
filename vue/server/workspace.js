var storage = require('./storage');

exports.getWorkspaces = function(user, callback) {
  return storage.listFiles([user, "workspaces"],  callback);
}
exports.getModel = function(user, modelName, callback) {
  return storage.getFile([user, "workspaces" ,modelName], "model.json",  callback);
}
exports.saveModel = function(json, user, modelName, callback) {
  return storage.saveFile(json, [user, "workspaces" ,modelName], "model.json",  callback);
}
exports.getVisuals = function(user, modelName, callback) {
  return storage.getFile([user, "workspaces" ,modelName], "visuals.json",  callback);
}
exports.saveVisuals = function(json, user, modelName, callback) {
  return storage.saveFile(json, [user, "workspaces" ,modelName], "visuals.json",  callback);
}
exports.getWindow = function(user, modelName, callback) {
  return storage.getFile([user, "workspaces" ,modelName], "window.json",  callback);
}
exports.saveWindow = function(json, user, modelName, callback) {
  return storage.saveFile(json, [user, "workspaces" ,modelName], "window.json",  callback);
}
exports.getDataSources = function(user, modelName, callback) {
  return storage.getFile([user, "workspaces" ,modelName], "datasources.json",  callback);
}
exports.saveDataSources = function(json, user, modelName, callback) {
  return storage.saveFile(json, [user, "workspaces" ,modelName], "datasources.json",  callback);
}



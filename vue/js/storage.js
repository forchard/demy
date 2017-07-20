const storage = {
  getWorkspaces: function(cb) {
    this.jsonRequest("/workspaces", cb);
  }

  ,getWorkspace: function(ws, cb) {
    this.jsonRequest(`/workspaces/${ws}/model`, cb);
  }

  ,jsonRequest: function(path, cb) { 
    request = new XMLHttpRequest()
    request.onreadystatechange = function() {
      if (request.readyState === XMLHttpRequest.DONE) {
        // everything is good, the response is received
        if (request.status === 200) {
          cb(null, JSON.parse(request.responseText));
        } else {
          cb("Error on Request", null);
        }
      } else {
        // still not ready
      }
    }
    request.responseType = 'text';
    request.open('GET', path);
    request.send();
  }


}



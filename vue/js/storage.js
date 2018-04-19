const storage = {
  getWorkspaces: function(cb) {
    this.jsonRequest("/workspaces", cb);
  }

  ,getWorkspace: function(ws, cb) {
    this.jsonRequest(`/workspaces/${ws}/model`, cb);
  }
  //added by me
  ,getRefresh: function(ws, cb){
    return new Promise((resolve,reject)=> {
      path = `/workspaces/${ws}/refresh`;
      request = new XMLHttpRequest()
      request.onreadystatechange = function() {

        if (request.readyState === XMLHttpRequest.DONE) {
          // everything is good, the response is received
          if (request.status === 200) {
            console.log('refresh request completed')
            resolve()
          } else {
          }
        } else {
          // still not ready
        }
      }
      request.responseType = 'text';
      request.open('GET', path);
      request.send();
    })

  }
  //added by me
  ,jsonRequest: function(path, cb) {
    request = new XMLHttpRequest()
    request.onreadystatechange = function() {
      if (request.readyState === XMLHttpRequest.DONE) {
        // everything is good, the response is received
        if (request.status === 200) {
          cb(null, JSON.parse(request.responseText));
          console.log("done", request.responseText)
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

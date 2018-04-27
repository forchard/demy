const storage = {
  addWorkspace: function(ws, login, passwd, url, cb) {
    this.jsonRequest(`/workspaces/${encodeURI(ws)}/addWks?name=${encodeURI(ws)}&login=${encodeURI(login)}&psw=${encodeURI(passwd)}&url=${encodeURI(url)}`, cb);
  }

  ,getWorkspaces: function(cb) {
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
  ,getData: function(url,ws){

    return new Promise((resolve,reject)=>{
      path = `/workspaces/${ws}${url}/data`; // the missing slash is added in query
      request = new XMLHttpRequest()
      request.onreadystatechange = function() {
        if (request.readyState === XMLHttpRequest.DONE) {
          // everything is good, the response is received
          if (request.status === 200) {
            console.log('row data received')
            const rowData = JSON.parse(request.responseText)
            resolve(rowData)

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

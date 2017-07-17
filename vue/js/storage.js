
function getModels() {
  httpRequest = new XMLHttpRequest()
  httpRequest.onreadystatechange = function() {
    if (httpRequest.readyState === XMLHttpRequest.DONE) {
      // everything is good, the response is received
      if (httpRequest.status === 200) {
        // perfect!
        alert(httpRequest.responseText);
      } else {
      // there was a problem with the request,
      // for example the response may contain a 404 (Not Found)
      // or 500 (Internal Server Error) response code
      }
    } else {
      // still not ready
    }
  }
  httpRequest.open('GET', "http://localhost:3000/workspaces");
  httpRequest.send();
};



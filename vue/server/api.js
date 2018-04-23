var http = require('http')
    ,url = require('url')
    ,querystring = require('querystring')
    ,fs = require('fs')
    ,conf = require('./conf')
    ,storage = require('./storage')
    ,workspace = require('./workspace')
    ,model = require('./model')
    ,addWks = require('./addWks')

exports.server = () => {
  return http.createServer((req, res) => {
    var login = conf.user.login;
    var url_parts = url.parse(req.url);
    var path = decodeURIComponent((url_parts.pathname+'').replace(/\+/g, '%20'));
    var query = url_parts.query;
    console.log(`New Request from ${login}:${path} from ${req.connection.remoteAddress} > ${req.headers['x-forwarded-for'] || ' (no proxy)'}`);
    streamToString(req, conf.http.max_post_bytes, (post, err) => {
    if(err)
      return unexpectedError(res, err);
    if(query && post)
      query = query + "&"+post;
    else if(post)
      query = post
      query = querystring.parse(query);

    if(path==="/workspaces") {
      console.log(`${login} is asking listing workspaces`)
      workspace.getWorkspaces(login, (err, files) => {
      if(err) return badRequest(res, err);
      res.statusCode = 200;
      res.write("[");
      for(var i=0; i<files.length;i++) {
        if(i>0) res.write(",");
        res.write(`"${files[i]}"`);
      }
      return res.end("]");
      });
    }
    else if(path.startsWith("/workspaces/") && path.endsWith("/model")) {
      path = path.substring("/workspaces/".length);
      path = path.substring(0, path.length - "/model".length)
      if(!storage.soundFileName(path))
        return notFound(res);
      console.log(`${login} is asking for model: ${ path }`)
      workspace.getModel(login, path, (err, fd) => {
      if(err) return badRequest(res, err);
      res.statusCode = 200;
      if(!fd) return res.end("{}");
      fd.pipe(res);
      });
    }
    else if(path.startsWith("/workspaces/") && path.endsWith("/refresh")) {
      path = path.substring("/workspaces/".length);
      path = path.substring(0, path.length - "/refresh".length)
      console.log(`${login} is asking for refresh on: ${ path }`)
        model.modelRefresh(path)
        .then(()=>{
          console.log('model created')
          res.statusCode = 200;
          return res.end("{refresh}")
        })
        .catch((err) => {
            if (err) throw err;
        })
    }

    else if(path.startsWith("/workspaces/") && path.endsWith("/addWks")) {
      console.log(`${login} is asking for adding a new workspace`)
          addWks.createWks(query.name,query.login,query.psw,query.url)
          .then(()=>{
            model.modelRefresh(query.name, query.login, query.psw, query.url)
          }).then(()=>{
          res.statusCode = 200;
          res.writeHead(302, { "Location": "http://" + req.headers['host'] + '/index.html' });
          return res.end()
          }).catch((err) => {
            if (err) throw err;
          })
    }

    else if(path.startsWith("/workspaces/") && path.endsWith("/visuals")) {
      path = path.substring("/workspaces/".length);
      path = path.substring(0, path.length - "/visuals".length)
      if(!storage.soundFileName(path))
        return notFound(res);
      console.log(`${login} is asking for refresh on: ${ path }`)
      workspace.getVisuals(login, path, (err, fd) => {
      if(err) return badRequest(res, err);
      res.statusCode = 200;
      if(!fd)  return res.end("{}");
      fd.pipe(res);
      });
    }
    else if(path.startsWith("/workspaces/") && path.endsWith("/window")) {
      path = path.substring("/workspaces/".length);
      path = path.substring(0, path.length - "/window".length)
      if(!storage.soundFileName(path))
        return notFound(res);
      console.log(`${login} is asking for window on: ${ path }`)
      workspace.getWindow(login, path, (err, fd) => {
      if(err) return badRequest(res, err);
      res.statusCode = 200;
      if(!fd)  return res.end("{}");
      fd.pipe(res);
      });
    }
    else if(path.startsWith("/workspaces/") && path.endsWith("/datasources")) {
      path = path.substring("/workspaces/".length);
      path = path.substring(0, path.length - "/datasources".length)
      if(!storage.soundFileName(path))
        return notFound(res);
      console.log(`${login} is asking for datasource on: ${ path }`)
      workspace.getDataSources(login, path, (err, fd) => {
      if(err) return badRequest(res, err);
      res.statusCode = 200;
      if(!fd)  return res.end("{}");
      fd.pipe(res);
      });
    }
    else if (path.startsWith("/workspaces/") && path.indexOf("/datasources/")>0) {
      path = path.substring("/workspaces/".length);
      model = path.substring(0, path.indexOf("/datasources/"))
      datasource = path.substring(path.indexOf("/datasources/") + "/datasources/".length)
      if(!storage.soundFileName(model))
        return notFound(res);
      console.log(`${login} is asking  datasource ${ datasource } on: ${ model }`)
      workspace.getDataSources(login, model, (err, fd) => {
      if(err) return badRequest(res, err);
      if(!fd)  return res.end("{}");
      streamToString(fd, conf.http.max_post_bytes, (data, err) => {
        if(err)
          return unexpectedError(res, err);
        var ds = JSON.parse(data).filter((val) => {return val.name === datasource})
        if(ds.length==0) {
          return notFound(res);
        }
        res.statusCode = 200;
        var connString = ds[0].connection_string;
        var driver = ds[0].driver;
        if(query.query){
          res.write(query.query)
        }

        res.end(/*JSON.stringify("")*/);
      })});
    }
    else {
      //static web server ignoring server folder
      if(path.startsWith("/server"))
      	return notFound(res);

      fs.readFile(conf.http.root+path, "binary", function(err, file) {
      if(err) {
        res.writeHead(500, {"Content-Type": "text/plain"});
        res.write(err + "\n");
        res.end();
        return;
      }

      if(path.endsWith(".html"))
        res.writeHead(500, {"Content-Type": "text/html"});
      else if(path.endsWith(".js"))
        res.writeHead(500, {"Content-Type": "application/x-javascript"});
      else if(path.endsWith(".css"))
        res.writeHead(500, {"Content-Type": "text/css"});
      res.writeHead(200);

      res.write(file, "binary");
      res.end();
      });
      return;
    }

//    res.write(`received a ${req.method} method \n`);
//    res.write(`called on  ${url.parse(req.url).pathname}\n`);
//    res.write(`asking  ${url.parse(req.url).query}\n`);
//    res.end('Bye!\n');
  })});
}

exports.start = (server) => {
  server.listen(conf.http.port, conf.http.hostname, () => {
  console.log(`Server running at http://${conf.http.hostname}:${conf.http.port}/`);
  });
}

function notFound(res) {
  res.statusCode = 404;
  res.end("Not Found\n");
}

function badRequest(res, err) {
  res.statusCode = 500;
  console.log(err);
  res.end("Bad Request\n");
}

function unexpectedError(res, err) {
  res.statusCode = 500;
  console.log(err);
  res.end("Unexpected Error\n");
}

function streamToString(stream, maxSize, callback) {
  const chunks = [];
  var size = 0;
  stream.on('data', (chunk) => {
    size = size + chunk.length;
    chunks.push(chunk.toString());
    if(size>maxSize)
      callback(null, new Error("Max size Allowed"));
  });
  stream.on('end', () => {
    callback(chunks.join(''));
  });
}

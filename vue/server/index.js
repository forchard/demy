var http = require('http')
    ,conf = require('./conf')
    ,workspace = require('./workspace')
    ,api = require('./api')

var server = api.server();
api.start(server);


const JDBC = require('jdbc');
const jinst = require('jdbc/lib/jinst');


if (!jinst.isJvmCreated()) {
  jinst.addOption("-Xrs");
  jinst.setupClasspath(['./drivers/mariadb-java-client-1.5.9.jar'
                        ]);
}

var config = {
  // Required 
  url: 'jdbc:mysql://localhost/neo',
 
  // Optional 
//  drivername: 'mariadb.jdbc',
  minpoolsize: 10,
  maxpoolsize: 100,
 
  // Note that if you sepecify the user and password as below, they get 
  // converted to properties and submitted to getConnection that way.  That 
  // means that if your driver doesn't support the 'user' and 'password' 
  // properties this will not work.  You will have to supply the appropriate 
  // values in the properties object instead. 
  user: 'fod',
  password: '',
  properties: {}
};

var neodb = new JDBC(config);

neodb.initialize(function(err) {
  if (err) {
    console.log(err);
  }
});

neodb.reserve(function(err, connObj) {
  // The connection returned from the pool is an object with two fields 
  // {uuid: <uuid>, conn: <Connection>} 
  if (err) {
    console.log(err);
  } else { 
    console.log("Using connection: " + connObj.uuid);
    // Grab the Connection for use. 
    var conn = connObj.conn;
    conn.createStatement(function(err, statement) {
      if (err) {
       console.log(err);
      } else {
       statement.setFetchSize(100, function(err) {
         if (err) {
           callback(err);
         } else {
           statement.executeQuery("SELECT id_event FROM event limit 1000;",
                                  function(err, resultset) {
             if (err) {
               callback(err)
             } else {
               resultset.toObjArray(function(err, results) {
                 if (results.length > 0) {
                   console.log("ID: " + results[0].id_event);
                 }
                 //callback(null, resultset);
               });
             }
           });
         }
        });
      }
    });
  }
});



function phrase() {
 
  this.requestFileSystem  = window.requestFileSystem || window.webkitRequestFileSystem;
  this.onError = function(e) {
    console.log('Error', e);
  };
  this.getFile = function() {
    var xhr = new XMLHttpRequest();
    xhr.open('GET', 'img/bullet_triangle_grey.png', true);
    xhr.responseType = 'blob';

    xhr.onload = function(e) {
      window.requestFileSystem(TEMPORARY, 1024 * 1024, function(fs) {
        fs.root.getFile('image.png', {create: true}, function(fileEntry) {
          fileEntry.createWriter(function(writer) {

            //writer.onwrite = function(e) { ... };
            //writer.onerror = function(e) { ... };

            var blob = new Blob([xhr.response], {type: 'image/png'});

            writer.write(blob);

          }, onError);
        }, onError);
      }, onError);
    };

    xhr.send();
  }

}

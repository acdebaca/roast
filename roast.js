var http = require('http');
var url  = require('url');
var fs = require('fs');

var server = http.createServer(function (req, res) {

  // Parse URI
  var resourceId = url.parse(req.url,true);
  var path = resourceId.pathname.split('/');
  var query = resourceId.query;

  var dataSource = path[1];
  //res.end(JSON.stringify(resourceId));

  // Read config file
  fs.readFile('roast.conf', function(errRoastConf, textRoastConf) {
    if (errRoastConf) throw errRoastConf;
    var roastConfig = JSON.parse(textRoastConf);

    // Match the request to the csv file
    if (dataSource in roastConfig.sources) {
      var csvFilePath = roastConfig.sources[dataSource].path;

      fs.readFile(csvFilePath, function(errCsvFile, textCsv) {
        if (errCsvFile) throw errCsvFile;

        // Figure out the index of the given query columns
        var rows = textCsv.toString().split('\n');
        var fields = rows[0].split(',');
        var fieldIndices = {};
        for (var j = 0; j < fields.length; j++) {
          fieldIndices[fields[j]] = j;
        }

        // Validate query
        for (var condition in query) {
          if (!fieldIndices.hasOwnProperty(condition)) {
            // Write out content header
            res.writeHead(200, {'Content-Type': 'text/plain'});
            res.end("Invalid column name \"" + condition + "\"");
            return;
          }
        }

        // Write out content header
        res.writeHead(200, {'Content-Type': 'text/plain'});

        // Write out column header row
        //res.write(rows[0] + "\n");

        // Write out rows that match query
        var resultCollection = [];
        for (var i = 1; i < rows.length; i++) {
          var match = true;
          var cells = rows[i].split(',');
          for (var condition in query) {
            if (cells[fieldIndices[condition]] != query[condition]) {
              match = false;
            }
          }

          if (match) {
            var resultEntry = {};
            for (var field in fields) {
              //res.write("Field: " + fields[field] + "\n");
              resultEntry[field] = cells[fieldIndices[fields[field]]];
            }
            resultCollection.push(resultEntry);
            //res.write(rows[i] + "\n");
          }
        }

        // Response is done.
        res.end(JSON.stringify(resultCollection));
      });
    } else if (dataSource == "") {
      res.writeHead(200, {'Content-Type': 'text/plain'});
      res.write("Available Data Sources:" + "\n");
      for (var ds in roastConfig.sources) {
        res.write(ds + "\n");
      }
      res.end();
    } else {
        res.writeHead(400, {'Content-Type': 'text/plain'});
        res.end("Unrecognized datasource \"" + dataSource + "\"");
    }
  });
});

server.listen(1337, '127.0.0.1');
console.log('Roast Server running at http://127.0.0.1:1337/');

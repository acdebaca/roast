var http = require('http');
var url  = require('url');
var fs = require('fs');

var knownSourceTypes = {};
knownSourceTypes.csv = true;
knownSourceTypes.http = true;

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

    // Validate roast config, track valid sources
    var validSources = {};
    if (!roastConfig.hasOwnProperty("sources")) {
      throw "ERROR: Roast config is missing \"sources\" section";
    }

    for (var source in roastConfig.sources) {
      var src = roastConfig.sources[source];
      if (!src.hasOwnProperty("type")) {
        console.log("WARNING: Roast config source \"" + source + "\" is missing a \"type\" property");
        continue;
      }

      // Make sure the type is known
      if (!(src.type in knownSourceTypes)) {
        console.log("WARNING: Data source \"" + source + "\" configured with unrecognized type \"" + src.type + "\"");
        continue;
      }

      // Data source is valid
      validSources[source] = roastConfig.sources[source];
    }
    //console.log("\n" + JSON.stringify(validSources) + "\n");
    //console.log("\n" + JSON.stringify(knownSourceTypes) + "\n");

    // Match the request to the csv file
    if (dataSource in validSources) {
      if (validSources[dataSource].type == "csv") {
        var csvFilePath = validSources[dataSource].path;

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
                resultEntry[fields[field]] = cells[fieldIndices[fields[field]]];
              }
              resultCollection.push(resultEntry);
            }
          }

          // Response is done.
          res.end(JSON.stringify(resultCollection));
        });
      } else if (validSources[dataSource].type == "http") {
        res.end("HTTP request");
      }
    } else if (dataSource == "") {
      res.writeHead(200, {'Content-Type': 'text/plain'});
      res.write("Available Data Sources:" + "\n");
      for (var ds in validSources) {
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

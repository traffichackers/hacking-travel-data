# Includes
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
qs = require 'pg-query-stream'
stream = require 'stream'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

getHistory = (client, callback) ->
  historyQuery = new qs("select pairid as pair_id, to_char(lastupdated,'YYYY-MM-DD HH24:MI:SS') as insert_time, traveltime as travel_time from history where lastupdated > '2014-10-01' limit 10;");
  historyStream = client.query(historyQuery);
  pg2csv = new stream.Transform( { objectMode: true } )
  csvStream = historyStream.pipe pg2csv
  utils.uploadFileStream csvStream, 'model_history.csv', () ->
    console.log 'upload complete'
  historyStream.on 'end', () ->
    console.log 'stream complete'
    utils.terminateConnection client, () ->
      console.log 'connection terminated'

pg2csv._transform = function (row, encoding, done) {
  keys = Object.keys row
  tempRow = []
  for key in keys
    tempRow.push row[key]
  historyCsv += tempRow.join(',')+'\n'

  lines.forEach(this.push.bind(this))
  done()
}

  console.log 'uploading history'
  utils.terminateConnection client, () ->
    callback null, historyCsv, 'model_history.csv'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  getHistory
]
async.waterfall(waterfallFunctions)

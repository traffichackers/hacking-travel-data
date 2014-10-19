# Includes
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
qs = require 'pg-query-stream'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

getHistory = (client, callback) ->
  var historyQuery = new qs("select pairid as pair_id, to_char(lastupdated,'YYYY-MM-DD HH24:MI:SS') as insert_time, traveltime as travel_time from history where lastupdated > '2014-10-01' limit 10;");
  var historyStream = client.query(historyQuery);

  stream.on 'end', () ->
    console.log("stream complete")
    utils.terminateConnection client, () ->
      console.log("connection terminated");

formatHistoryRow = () ->
  # Generate the CSV string
  historyCsv = ''
  for row in result.rows
    if historyCsv is ''
      keys = Object.keys(row)
      historyCsv = keys.join(',')
    tempRow = []
    for key in keys
      tempRow.push row[key]
    historyCsv += '\n'+tempRow.join(',')
  console.log 'uploading history'
  utils.terminateConnection client, () ->
    callback null, historyCsv, 'model_history.csv'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  getHistory
]
async.waterfall(waterfallFunctions)

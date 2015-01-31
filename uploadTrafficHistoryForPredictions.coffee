# Includes
async = require 'async'
qs = require 'pg-query-stream'
stream = require 'stream'
utils = require './utils'
dotenv = require 'dotenv'
dotenv.load()

getHistory = (client, callback) ->
  historyQuery = new qs "select pairid as pair_id, to_char(lastupdated,'YYYY-MM-DD HH24:MI:SS') as insert_time, traveltime as travel_time from history where lastupdated > '2014-01-01';"
  historyStream = client.query historyQuery
  pg2csv = new stream.Transform { objectMode: true }
  pg2csv._transform = (row, encoding, done) ->
    keys = Object.keys row
    if this.headersSet
      historyCsv = ''
    else
      historyCsv = keys.join ','
      this.headersSet = true
    tempRow = []
    tempRow.push row[key] for key in keys

    historyCsv += '\n' + tempRow.join ','
    this.push historyCsv
    done()
  csvStream = historyStream.pipe pg2csv
  utils.uploadFileStream csvStream, 'model_history.csv', () ->
    console.log 'upload complete'
  historyStream.on 'end', () ->
    console.log 'stream complete'
    utils.terminateConnection client, () ->
      console.log 'connection terminated'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  getHistory
]
async.waterfall(waterfallFunctions)

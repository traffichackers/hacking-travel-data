# Includes
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

getHistory = (client, callback) ->
  historyQuery = "select pairid, to_char(lastupdated,'YYYY-MM-DD HH24:MI') as lastupdated, stale, traveltime, speed, freeflow from history where pairid in (10356,10357,10358,10359,10360,10361,10363,10364,10496,10499);"
  client.query historyQuery, (err, result) ->
    console.log 'history received'

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
      callback null, historyCsv, 'i90_itf_data.csv'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  getHistory,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

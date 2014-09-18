# Includes
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

prepareData = (client, callback) ->

  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  preparationQueries = ["drop table history2;",
    "select * into history2 from (select distinct * from (select * from history3 union select * from history) as combo) as distinctHistory;",
    "CREATE INDEX pairidIdx ON history2 (pairid);"];

  async.eachSeries preparationQueries, issueQuery, (err) ->
    callback(null, client)

getHistory = (client, callback) ->
  historyQuery = "select pairid, to_char(lastupdated,'YYYY-MM-DD HH24:MI') as lastupdated, stale, traveltime, speed, freeflow from history2 where pairid in (10356,10357,10358,10359,10360,10361,10363,10364,10496,10499);"
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
  prepareData,
  getHistory,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

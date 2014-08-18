# Includes
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

# Create the current.json object from the download
getHistory = (callback) ->
  utils.initializeConnection (err, client) ->
    historyQuery = 'select * from history2 limit 1000';
    client.query historyQuery, (err, result) ->

      # Generate the CSV string
      historyCsv = ''
      for row in result.rows
        if historyCsv is ''
          keys = Object.keys(row)
          historyCsv = keys.join(',')
        tempRow = []
        for key in keys
          tempRow.push row[key]
          console.log tempRow
        historyCsv += '\n'+tempRow.join(',')

      utils.terminateConnection client, () ->
        callback null, historyCsv, 'history.csv'

# Start the Waterfall
waterfallFunctions = [
  getHistory,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

# Includes
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
utils = require './utils.coffee'  # Require
config = require './config.json'  # Server Configuration
betterDescriptions = require './data/betterDescriptions.json'   # Replacement descriptions for pair ids

# Query the database and add percentiles for each pair id
getAllPercentileQuery = (callback) ->
  query = 'select * from percentiles_all where recordcount > 100 order by pairid, lastUpdated'
  fileName = 'percentilesDow.json'
  callback null, query, fileName

getDowPercentileQuery = (callback) ->
  query = 'select * from percentiles_dow where recordcount > 100 order by pairid, lastUpdated'
  fileName = 'percentilesAll.json'
  callback null, query, fileName

issueQuery = (query, fileName, callback) ->
  utils.initializeConnection (err, client) ->
    console.log 'issuing query'
    client.query query, (err, result) ->
      console.log 'results received'
      callback null, result, fileName
      utils.terminateConnection client, (err) ->
      
createPercentiles = (result, fileName, callback) ->
  console.log 'processing percentiles'
  pairData = {}
  
  for row in result.rows
    # Extract Data
    if !pairData[row.pairid]?
      pairData[row.pairid] = {}
    if !pairData[row.pairid]['percentiles']?
      pairData[row.pairid]['percentiles'] = {}
    if row.dow?
      if !pairData[row.pairid]['percentiles'][row.dow]?
        pairData[row.pairid]['percentiles'][row.dow] = {}    
      percentiles = pairData[row.pairid]['percentiles'][row.dow]
    else
      percentiles = pairData[row.pairid]['percentiles']
    for key, i in ['p10', 'p30', 'p50', 'p70', 'p90']
      lastUpdated = parseInt(row.lastupdated.substr(0,5).replace(':',''))
      travelTime = Math.round(row[key])
      percentiles[key] = [] if !percentiles[key]?
      percentiles[key].push {1: lastUpdated, 2: travelTime}  
  console.log 'percentiles processed'
  callback null, pairData, fileName

# Start the Waterfall
waterfallFunctions = [
  getDowPercentileQuery,
  issueQuery,
  createPercentiles,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

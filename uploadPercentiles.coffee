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
  pairData = {}
  callback null, query, pairData

getDowPercentileQuery = (pairData, callback) ->
  query = 'select * from percentiles_dow where recordcount > 100 order by pairid, lastUpdated'
  callback null, query, pairData

issueQuery = (query, pairData, callback) ->
  utils.initializeConnection (err, client) ->
    console.log 'issuing query'
    client.query query, (err, result) ->
      console.log 'results received'
      callback null, result, pairData
      utils.terminateConnection client, (err) ->
      
createPercentiles = (result, pairData, callback) ->
  console.log 'processing percentiles'
  
  for row in result.rows

    # Initialize Objects
    pairData[row.pairid] = {} if !pairData[row.pairid]? 
    pairData[row.pairid].percentiles = {} if !pairData[row.pairid].percentiles?
    percentiles = pairData[row.pairid].percentiles
    if row.dow?
      percentiles.dow = {} if !percentiles.dow?
      percentiles.dow[row.dow] = [] if !percentiles.dow[row.dow]?
      percentiles = percentiles.dow
    else
      percentiles.all = {} if !percentiles.all?
      percentiles = percentiles.all
    
    # Load Data
    for key, i in ['p10', 'p30', 'p50', 'p70', 'p90']
      lastUpdated = row.lastupdated.substr(0,5)
      travelTime = Math.round(row[key])
      percentiles[key] = [] if !percentiles[key]?
      percentiles[key].push {'x': lastUpdated, 'y': travelTime}
      
  console.log 'percentiles processed'
  callback null, pairData
  
uploadFiles = (pairData, callback) ->
  # Process into Array
  uploadList = []
  for fileName, pairDatum of pairData
    uploadList.push {'content':pairDatum, 'name':fileName+'.json'}

  # Upload
  utils.uploadFiles uploadList  

# Start the Waterfall
waterfallFunctions = [
  getAllPercentileQuery,
  issueQuery,
  createPercentiles,
  getDowPercentileQuery,
  issueQuery,
  createPercentiles,
  uploadFiles
]
async.waterfall(waterfallFunctions)

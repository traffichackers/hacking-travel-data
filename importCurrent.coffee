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

# Get the raw MassDOT XML file
getCurrentData = (callback) ->
  httpCallback = (response) ->
    str = ''
    response.on 'data', (chunk) ->
      str += chunk
    response.on 'end', () ->
      callback null, str
  http.request(config.massDotConfig, httpCallback).end()

# Create the current.json object from the download
createCurrent = (data, callback) ->
  attributes = {'Title':'title','Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
  utils.parseMassDotXml data, (results) ->
    current = {}
    current.lastUpdated = results.lastUpdated
    current.pairData = {}

    # Iterate over pair ids
    for pair in results.pairData
      processedPairData = {}
      pairId = pair['PairID'][0]
      processedPairData['title'] = betterDescriptions[pairId]
      for mDotName, internalName of attributes
        if !processedPairData[internalName]?
          processedPairData[internalName] = pair[mDotName][0]
      current.pairData[pairId] = processedPairData
    callback null, current    

# Query the database and add percentiles for each pair id
addPercentiles = (current, callback) ->
  utils.initializeConnection (err, client) ->
    query = 'select * from percentiles_all where recordcount > 100 order by pairid, lastUpdated'
    client.query query, (err, result) ->
      for row in result.rows
        # Extract Data
        if current.pairData[row.pairid]?  
          if !current.pairData[row.pairid]['percentiles']?
            current.pairData[row.pairid]['percentiles'] = {}  
          percentiles = current.pairData[row.pairid]['percentiles']
          for key, i in ['p10', 'p30', 'p50', 'p70', 'p90']
            lastUpdated = row.lastupdated.substr(0,5)
            travelTime = Math.round(row[key])
            percentiles[key] = [] if !percentiles[key]?
            percentiles[key].push {x: lastUpdated, y: travelTime}  
      utils.terminateConnection client, (err) ->
        callback null, current

stringifyData = (current, callback) ->  
  currentText = JSON.stringify current
  callback null, currentText,'current.json'

uploadFile = (fileText, fileName, callback) ->
  fileBuffer = new Buffer(fileText)
  ftpClient = new ftp
  ftpClient.on 'ready', () ->
    console.log 'uploading current.json'
    ftpClient.put fileBuffer, fileName, (err) ->
      console.log 'finished uploading current.json'
      throw err if err
      ftpClient.end()
      callback null
  ftpClient.connect config.ftpConfig

# Start the Waterfall
waterfallFunctions = [
  getCurrentData,
  createCurrent,
  addPercentiles,
  stringifyData,
  uploadFile
]
async.waterfall(waterfallFunctions)

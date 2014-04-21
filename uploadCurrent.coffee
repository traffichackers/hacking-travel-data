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

  date = new Date()
  a = date.toISOString().replace(/[T\-:]/g,'_')
  fileName = a.slice(0,7)+a.slice(8,11)+a.slice(11,13)+a.slice(14,16)
  fullFileName = config.xmlExportPath+fileName+'.xml'
  console.log fullFileName
  fd = fs.openSync fullFileName, 'a', undefined
  fs.writeSync fd, data, undefined, undefined
  
  utils.parseMassDotXml data, (results) ->
    current = {}
    current.lastUpdated = results.lastUpdated
    current.pairData = {}
    currentInsertQuery = ""
    
    # Iterate over pair ids
    for pair in results.pairData
      processedPairData = {}
      
      if !isNaN(pair['TravelTime'][0])      
        processedPairData['pairId'] = pair['PairID'][0]
        processedPairData['stale'] = if pair.Stale[0] is 1 then true else false
        processedPairData['travelTime'] = pair['TravelTime'][0]
        processedPairData['speed'] = pair['Speed'][0]
        processedPairData['freeFlow'] = pair['FreeFlow'][0]
        processedPairData['title'] = betterDescriptions[processedPairData.pairId]
 
        currentInsertQuery += "insert into history (pairId, lastUpdated, stale, travelTime, speed, freeFlow) values ("+processedPairData.pairId+",'"+current.lastUpdated+"',"+processedPairData.stale+","+processedPairData.travelTime+","+processedPairData.speed+","+processedPairData.freeFlow+");\n"
        current.pairData[processedPairData.pairId] = processedPairData
        
    utils.initializeConnection (err, client) ->
      client.query currentInsertQuery, (err, result) ->
        utils.terminateConnection client, () ->
    callback null, current, 'current.json'


# Start the Waterfall
waterfallFunctions = [
  getCurrentData,
  createCurrent,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

# Includes
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
async = require 'async'
csv = require 'csv'
utils = require './utils'  # Require
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

  # Write XML to disk
  fullFileName = config.xmlExportPath+fileName+'.xml'
  fd = fs.openSync fullFileName, 'a', undefined
  fs.writeSync fd, data, undefined, undefined
  console.log fullFileName + ' written'

  # Parse XML and insert
  utils.parseMassDotXml data, (results) ->
    current = {}
    current.lastUpdated = results.lastUpdated
    current.pairData = {}
    currentInsertQuery = ""

    # Iterate over pair ids
    for pair in results.pairData
      processedPairData = {}

      if utils.isValidPair(pair)
        processedPairData['pairId'] = pair['PairID'][0]
        processedPairData['stale'] = if pair.Stale[0] is 1 then true else false
        processedPairData['travelTime'] = pair['TravelTime'][0]
        processedPairData['speed'] = pair['Speed'][0]
        processedPairData['freeFlow'] = pair['FreeFlow'][0]
        processedPairData['title'] = betterDescriptions[processedPairData.pairId]
        processedPairData.title = pair['Title'][0] if !processedPairData.title?

        currentInsertQuery += "insert into history (pairId, lastUpdated, stale, travelTime, speed, freeFlow) values ("+processedPairData.pairId+",'"+current.lastUpdated+"',"+processedPairData.stale+","+processedPairData.travelTime+","+processedPairData.speed+","+processedPairData.freeFlow+");\n"
        current.pairData[processedPairData.pairId] = processedPairData

    utils.initializeConnection (err, client) ->
      client.query currentInsertQuery, (err, result) ->
        callback null, current, client

getCurrentPredictions = (current, client, callback) ->
  console.log 'integrating current predictions'

  # Read the current predictions file
  fs.readFile config.currentPredictionsPath, (err, currentPredictionsText) ->
    if err
      throw err

    pairData = current.pairData
    currentPredictions = JSON.parse currentPredictionsText

    for pairId, pairPredictions of currentPredictions
      # Initialize objects, if necessary
      if pairData[pairId]?
        pairData[pairId].predictions = [] if !pairData[pairId].predictions?s
        for predictedValue, i in pairPredictions['50']
          pairData[pairId].predictions.push Math.ceil predictedValue

    callback null, current, client

getTodayData = (current, client, callback) ->
  todayDataQuery = "select pairId, lastUpdated::timestamp::time, stale, travelTime, speed, freeFlow from history where lastUpdated::date = now()::date order by pairId, lastUpdated"
  console.log 'pulling today data from database'

  client.query todayDataQuery, (err, result) ->
    console.log 'today data pulled'
    pairData = current.pairData
    for row in result.rows

      # Initialize objects, if necessary
      if pairData[row.pairid]?
        pairData[row.pairid].today = [] if !pairData[row.pairid].today?

        # Delete Objects
        lastUpdated = row.lastupdated.substr(0,5)
        travelTime = Math.round(row.traveltime)
        pairData[row.pairid].today.push {'x': lastUpdated, 'y': travelTime}

    utils.terminateConnection client, () ->
      callback null, current, 'current.json'

# Start the Waterfall
waterfallFunctions = [
  getCurrentData,
  createCurrent,
  getCurrentPredictions,
  getTodayData,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

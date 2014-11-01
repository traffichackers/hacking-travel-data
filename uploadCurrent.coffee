# Includes
fs = require 'fs'
xml2js = require 'xml2js'
http = require 'http'
async = require 'async'
csv = require 'csv'
zlib = require 'zlib'
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
  fullFileName = config.xmlDirectory+'hackingtravel/'+fileName+'.xml.gz'

  zlib.gzip data, (_, result) ->
    fs.writeFile(fullFileName, result)
    console.log fullFileName + ' written'

    # Parse XML and insert
    utils.parseMassDotXml data, (results) ->
      current = {}

      lastUpdated = new Date results.lastUpdated
      current.lastUpdated = lastUpdated.toLocaleString()
      current.pairData = {}
      currentInsertQuery = ''
      secondaryCurrentInsertQuery = ''

      # Iterate over pair ids
      for pair in results.pairData
        processedPairData = {}

        if utils.isValidPair(pair)
          processedPairData['pairId'] = pair['PairID'][0]
          processedPairData['stale'] = if pair.Stale[0] is 1 then true else false
          processedPairData['travelTime'] = Math.round(pair['TravelTime'][0],2);
          processedPairData['speed'] = pair['Speed'][0]
          processedPairData['freeFlow'] = pair['FreeFlow'][0]
          processedPairData['title'] = betterDescriptions[processedPairData.pairId]
          processedPairData.title = pair['Title'][0] if !processedPairData.title?

          currentInsertQuery += "insert into history (pairId, lastUpdated, stale, travelTime, speed, freeFlow) values ("+processedPairData.pairId+",'"+results.lastUpdated+"',"+processedPairData.stale+","+processedPairData.travelTime+","+processedPairData.speed+","+processedPairData.freeFlow+");\n"
          secondaryCurrentInsertQuery += "insert into "+config.historyStagingTableNameDeduplicated+" (pairId, lastUpdated, stale, travelTime, speed, freeFlow) values ("+processedPairData.pairId+",'"+results.lastUpdated+"',"+processedPairData.stale+","+processedPairData.travelTime+","+processedPairData.speed+","+processedPairData.freeFlow+");\n"
          current.pairData[processedPairData.pairId] = processedPairData

      # Insert into primary data store
      utils.initializeConnection (err, client) ->
        client.query currentInsertQuery, (err, result) ->

          # Insert into secondary data store
          client.query secondaryCurrentInsertQuery, (err, result) ->

            # Close the database connection
            utils.terminateConnection client, () ->
              callback null, JSON.stringify(current), 'data/current.json'

# Start the Waterfall
waterfallFunctions = [
  getCurrentData,
  createCurrent,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

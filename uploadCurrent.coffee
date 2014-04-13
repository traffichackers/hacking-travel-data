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
    callback null, current, 'current.json'


# Start the Waterfall
waterfallFunctions = [
  getCurrentData,
  createCurrent,
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

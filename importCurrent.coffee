# Npm Includes
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
pg = require 'pg' 
async = require 'async'
csv = require 'csv'

# Local Includes
utils = require './utils.coffee'
config = require './config.json'
betterDescriptions = require './data/betterDescriptions.json'

timeSeries = []

uploadFile = (inputFile,outputFileName) ->
  ftpClient = new ftp
  ftpClient.on 'ready', () ->
    ftpClient.put inputFile, 'current.json', (err) ->
      throw err if err
      ftpClient.end()
  ftpClient.connect config.ftpConfig

parser = new xml2js.Parser()
  
# Functions
getCurrentData = () ->
  callback = (response) ->
    str = ''
    response.on 'data', (chunk) ->
      str += chunk
    response.on 'end', () ->
      extractMetadata null, str
  http.request(config.massDotConfig, callback).end()

extractMultipleFileData = () ->
  metaData = []
  files = fs.readdir __dirname+'/data', (err, files) ->
    for file in files
      extractSingleFileData file

extractSingleFileData = (file) ->
  fs.readFile __dirname+'/data/'+file, 'ascii', extractMetadata
    
extractMetadata = (err, data) ->
  attributes = {'Title':'title','Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
  utils.parseMassDotXml data, parser, (results) ->
    processedTravelData = {}
    processedTravelData.pairData = {}
    processedTravelData.lastUpdated = results.lastUpdated

    # Iterate over pair ids
    for pair in results.pairData
      processedPairData = {}
      pairId = pair['PairID'][0]
      processedPairData['title'] = betterDescriptions[pairId]
      for mDotName, internalName of attributes
        if !processedPairData[internalName]?
          processedPairData[internalName] = pair[mDotName][0]
      processedTravelData.pairData[pairId] = processedPairData
    processedTravelDataText = JSON.stringify(processedTravelData)
    fileBuffer = new Buffer(processedTravelDataText)
    uploadFile(fileBuffer,'current.json')

# Init
getCurrentData()

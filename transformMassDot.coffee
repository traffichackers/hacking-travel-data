# Library
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
pg = require 'pg' 
async = require 'async'
csv = require 'csv'
utils = require './utils.coffee'

# Config
config = require './config.json'

attributes = {'Title':'title','Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
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
  utils.parseMassDotXml data, parser, (results) ->
    processedTravelData = {}
    processedTravelData.pairData = {}
    processedTravelData.lastUpdated = results.lastUpdated
    for pair in results.pairData
      processedPairData = {}
      pairId = pair['PairID'][0]
      for mDotName, internalName of attributes
        processedPairData[internalName] = pair[mDotName][0]
        processedTravelData.pairData[pairId] = processedPairData
    processedTravelDataText = JSON.stringify(processedTravelData)
    fileBuffer = new Buffer(processedTravelDataText)
    uploadFile(fileBuffer,'current.json')

# Init
getCurrentData()

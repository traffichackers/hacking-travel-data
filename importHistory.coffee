# Library
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
pg = require 'pg'
async = require 'async'
csv = require 'csv'
zlib = require 'zlib'

# Config
config = require './config.json'

# Connection
initializeConnection = (callback) ->
  console.log 'initializing connnection'
  connectionString = config.postgresConnectionString
  client = new pg.Client(config.connectionOptions)
  client.connect (err) ->
    if err
      return console.error 'could not connect to postgres', err
    else
      console.log 'connection initialized'
      callback null, client

dropHistoryTable = (client, callback) ->
  console.log 'initializing tables'
  client.query config.historyCheckQuery, (err, result) ->
    callback null, client

createHistoryTable = (client, callback) ->
  client.query config.historyCreateQuery, (err, result) ->
    callback null, client

terminateConnection = (client, callback) ->
  client.end()
  callback null, client

# Data Functions
importHackReduceData = (client, callback) ->
  hackReducePath =
  hackReduceCsv = fs.readFileSync config.hackReducePath, 'ascii'
  startIndex = 0
  insertHackReduceData(hackReduceCsv, startIndex, client, -1, callback)

insertHackReduceData = (hackReduceCsv, startIndex, client, oldPercentProcessed, callback) ->
  hackReduceQuery = "begin;\n"
  while hackReduceQuery.length < 100000
    endIndex = hackReduceCsv.indexOf('\n',startIndex)
    if endIndex is -1
      callback null, client
      break
    numChars = endIndex-startIndex
    datum = hackReduceCsv.substr(startIndex,numChars).split(',')
    pairId = datum[0]
    lastUpdated = datum[1]
    travelTime = datum[2]
    if !isNaN(travelTime)
      hackReduceQuery += "insert into history3 (pairId, lastUpdated, travelTime) values ("+pairId+",'"+lastUpdated+"',"+travelTime+");\n"
    startIndex = endIndex+1
  hackReduceQuery += "end;\n"
  client.query hackReduceQuery, (err, result) ->
    percentProcessed = parseInt(endIndex/hackReduceCsv.length*100)
    if percentProcessed isnt oldPercentProcessed
      console.log  percentProcessed + "% processed"
    insertHackReduceData(hackReduceCsv, endIndex+1, client, percentProcessed, callback)

importManuallyDownloadedData = (client, callback) ->
  directories = fs.readdirSync config.manualImportPath
  xmlFiles = []
  for directory in directories
    files = fs.readdirSync '/home/andrew/xml/'+directory
    for file, i in files
      if file.slice(-7) is '.xml.gz'
        xmlFiles.push '/home/andrew/xml/'+directory+'/'+file
  startFileId = 0
  parser = new xml2js.Parser()
  insertManuallyDownloadedData xmlFiles, client, startFileId, parser

insertManuallyDownloadedData = (xmlFiles, client, startFileId, parser, callback) ->
  if xmlFiles.length-1 > startFileId
    xmlFile = xmlFiles[startFileId]
    buffer = fs.readFileSync xmlFile, 'ascii'
    zlib.gunzip(buffer, (err, data) ->
      if data.slice(0,5) == '<?xml'
        manualDownloadsQuery = "begin;\n"
        parser.parseString data, (err, result) ->
          travelData = result.btdata?.TRAVELDATA[0]
          lastUpdated = travelData?.LastUpdated[0]
          pairData = travelData?.PAIRDATA

          for pair in pairData
            pairId = pair['PairID'][0]
            stale = pair['Stale'][0]
            stale = if stale is 1 then true else false
            travelTime = pair['TravelTime'][0]
            speed = pair['Speed'][0]
            freeFlow = pair['FreeFlow'][0]
            if !isNaN(travelTime)
              manualDownloadsQuery += "insert into history3 (pairId, lastUpdated, stale, travelTime, speed, freeFlow) values ("+pairId+",'"+lastUpdated+"',"+stale+","+travelTime+","+speed+","+freeFlow+");\n"
        manualDownloadsQuery += "end;\n"
        client.query manualDownloadsQuery, (err, result) ->
          console.log "file " + startFileId + " processed"
          insertManuallyDownloadedData(xmlFiles, client, startFileId+1, parser, callback)
      else
        insertManuallyDownloadedData(xmlFiles, client, startFileId+1, parser, callback)
  else
    callback null, client

# Utilities
getDirs = (rootDir) ->
  files = fs.readdirSync(rootDir)
  dirs = []

  for file in files
    if file[0] != '.'
      filePath = "#{rootDir}/#{file}"
      stat = fs.statSync(filePath)
      if (stat.isDirectory())
        dirs.push(file)

  return dirs

# CSV Generation
attributes = {'Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
extractMultipleFileData = () ->
  fd = fs.openSync 'data.csv', 'a', undefined
  metaData = []
  directories = getDirs __dirname+'/xml'
  for directory in directories
    files = fs.readdirSync __dirname+'/xml/'+directory
    for file, i in files
      if file.slice(-4) is '.xml'
        data = fs.readFileSync __dirname+'/xml/'+directory+'/'+file, 'ascii'
        if data.slice(0,5) == '<?xml'
          parser.parseString data, (err, result) ->
            travelData = result.btdata?.TRAVELDATA[0]
            lastUpdated = travelData?.LastUpdated[0]
            pairData = travelData?.PAIRDATA
            for pair in pairData
              travelDataString = ''
              travelDataString += lastUpdated
              pairId = pair['PairID'][0]
              travelDataString += ','+pairId
              for mDotName, internalName of attributes
                travelDataString += ','+pair[mDotName][0]
              travelDataString += '\n'
              fs.writeSync fd, travelDataString, undefined, undefined
      if i%10 is 0
        console.log i + " files written"
  return true

# Start the Waterfall
waterfallFunctions = [
  importConfig,
  initializeConnection,
  dropHistoryTable,
  createHistoryTable,
  importHackReduceData,
  importManuallyDownloadedData,
  terminateConnection
]
async.waterfall(waterfallFunctions)

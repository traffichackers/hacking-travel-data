# Npm Includes
fs = require 'fs'
ftp = require 'ftp'
pg = require 'pg'
xml2js = require 'xml2js'
http = require 'http'
csv = require 'csv'
parser = new xml2js.Parser()

# Local Includes
config = require './config.json'

# Private Utility Methods
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

extractMultipleFileData = () ->
  metaData = []
  files = fs.readdir __dirname+'/data', (err, files) ->
    for file in files
      extractSingleFileData file

extractSingleFileData = (file) ->
  fs.readFile __dirname+'/data/'+file, 'ascii', extractMetadata


# FTP
initializeFtpConnection = (callback) ->
    ftpClient = new ftp
    ftpClient.on 'ready', () ->
      callback(ftpClient)
    ftpClient.connect config.ftpConfig

coreUpload = (ftpClient, fileList, counter, callback) ->
    # Extract Names
    fileData = fileList[counter].content
    fileName = fileList[counter].name

    # Process and Upload
    fileText = JSON.stringify fileData
    fileBuffer = new Buffer(fileText)
    console.log 'uploading '+fileName
    ftpClient.put fileBuffer, fileName, (err) ->
      console.log 'finished uploading '+fileName
      throw err if err
      if counter is fileList.length-1
        callback null
      else
        coreUpload ftpClient, fileList, counter+1, callback

module.exports =

  isValidPair: (pair) ->
    validPair = true
    criticalFields = ['TravelTime', 'Speed', 'FreeFlow']
    for criticalField in criticalFields
      if isNaN(pair[criticalField][0])
        validPair = false
      else if pair[criticalField][0] is ''
        validPair = false
    validPair

  uploadFiles: (fileList, callback) ->
    initializeFtpConnection (ftpClient) ->
      counter = 0
      coreUpload ftpClient, fileList, counter, callback

  uploadFile: (fileData, fileName, callback) ->
    fileText = JSON.stringify fileData
    fileBuffer = new Buffer(fileText)
    initializeFtpConnection (ftpClient) ->
      console.log 'uploading '+fileName
      ftpClient.put fileBuffer, fileName, (err) ->
        console.log 'finished uploading '+fileName
        throw err if err
        ftpClient.end()
        callback null

  initializeConnection: (callback) ->
    console.log 'initializing connnection'
    connectionString = config.postgresConnectionString
    client = new pg.Client(config.connectionOptions)
    client.connect (err) ->
      if err
        return console.error 'could not connect to postgres', err
      else
        console.log 'connection initialized'
        callback null, client

  terminateConnection: (client, callback) ->
    client.end()
    callback null


  # XML Parser
  parseMassDotXml:  (data, callback) ->
    if data.slice(0,5) == '<?xml'
      parser.parseString data, (err, result) ->
        travelData = result.btdata?.TRAVELDATA[0]
        lastUpdated = travelData?.LastUpdated[0]
        pairData = travelData?.PAIRDATA
        callback({'lastUpdated':lastUpdated, 'pairData':pairData})

  # CSV Generation
  writeXml: () ->
    attributes = {'Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
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

# Npm Includes
fs = require 'fs'
ftp = require 'ftp'
pg = require 'pg'
xml2js = require 'xml2js'
http = require 'http'
csv = require 'csv'
aws = require 'aws-sdk'
dotenv = require 'dotenv'
zlib = require 'zlib'
parser = new xml2js.Parser()
dotenv.load()

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
    ftpConfig = {
      "host": process.env.FTP_CONFIG_HOST,
      "user": process.env.FTP_CONFIG_USER,
      "password": process.env.FTP_PASSWORD
    }
    ftpClient.connect ftpConfig

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

# AWS
uploadAwsFile = (fileData, fileName, callback) ->
  zlib.gzip fileData, (error, compressedFileData) ->
    dotenv.load()
    s3 = new aws.S3()
    aws.config.update({
      accessKeyId: process.env.AWS_ACCESS_KEY_ID,
      secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
      region: process.env.AWS_REGION
    })
    params =
      Bucket: process.env.AWS_BUCKET
      Key: fileName
      Body: compressedFileData
      ContentType: "application/json"
      ContentEncoding: "gzip"

    s3.putObject params, (err, data) ->
      if err
        console.log err, err.stack
      else
        console.log data

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

  uploadFileStream: (fileStream, fileName, callback) ->
    initializeFtpConnection (ftpClient) ->
      console.log 'uploading '+fileName
      ftpClient.put fileStream, fileName, (err) ->
        console.log 'finished uploading '+fileName
        throw err if err
        ftpClient.end()
        callback null

  uploadFile: (fileText, fileName, callback) ->

    # Convert to string if needed
    if typeof fileText is "object"
      fileText = JSON.stringify fileText
    #fileBuffer = new Buffer(fileText)
    #initializeFtpConnection (ftpClient) ->
    #  console.log 'uploading '+fileName+' to ftp'
    #  ftpClient.put fileBuffer, fileName, (err) ->
    #    console.log 'finished uploading '+fileName+' to ftp'
    #    throw err if err
    #    ftpClient.end()
    #    console.log 'uploading '+fileName+' to aws'
    uploadAwsFile fileText, fileName, () ->
      console.log 'finished uploading '+fileName+' to aws'
      callback null

  initializeConnection: (callback) ->
    console.log 'initializing connnection'
    connectionString = process.env.POSTGRES_CONNECTION_STRING
    postGresConnectionOptions = {
      "host": process.env.POSTGRES_CONNECTION_HOST,
      "user": process.env.POSTGRES_CONNECTION_USER,
      "password": process.env.POSTGRES_CONNECTION_PASSWORD,
      "database": process.env.POSTGRES_CONNECTION_DATABASE
    }
    client = new pg.Client(postGresConnectionOptions)
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

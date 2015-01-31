# Libraries
fs = require 'fs'
async = require 'async'
zlib = require 'zlib'
utils = require './utils'

# Config
config = require './config.json'

# Data Functions
prepareTables = (client, callback) ->
  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  preInsertQueries = [
    "drop table if exists model_results;",
    "create table model_results (predictionStartTime timestamp with time zone,
       predictionTime timestamp with time zone, pairId integer, percentile varchar(3),
       predictedSpeed double precision);"
  ]

  async.eachSeries preInsertQueries, issueQuery, (err) ->
    console.log 'fresh model_results table created'
    callback null, client

getFiles = (client, callback) ->
  files = fs.readdirSync config.modelResultsPath
  getNextFile files, client, callback

getNextFile = (files, client, callback) ->
  if files.length > 0
    importFile files[0], client, () ->
      files.shift()
      getNextFile files, client, callback
  else
    callback null, client

importFile = (file, client, callback) ->
  if file.slice(-8) is '.json.gz'
    prediction = config.modelResultsPath+'/'+file
    buffer = fs.readFileSync prediction
    zlib.gunzip buffer, (err, data) ->
      console.log "error:" + err if err
      modelResultsQuery = ""
      data = JSON.parse(data)
      for pairId of data
        if pairId isnt 'Start'
          for percentile of data[pairId]
            predictionStartTime = new Date(data['Start'])
            predictionStartTimeString = predictionStartTime.toISOString()
            percentileData = data[pairId][percentile]
            if percentileData isnt null
              predictionTime = predictionStartTime
              for predictedSpeed in percentileData
                modelResultsQuery += "insert into model_results (predictionStartTime,
                  predictionTime, pairId, percentile, predictedSpeed) values
                  ('"+predictionStartTimeString+"', '"+predictionTime.toISOString()+"',
                  "+pairId+", '"+percentile+"', "+predictedSpeed+");\n"
                predictionTime = new Date(predictionTime.getTime() + 300)
      client.query modelResultsQuery, (err, result) ->
        if err
          console.log err
        else
          console.log file + " processed"
        callback()

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  prepareTables,
  getFiles,
  utils.terminateConnection
]
async.waterfall(waterfallFunctions)
# Libraries
fs = require 'fs'
async = require 'async'
zlib = require 'zlib'
utils = require './utils'
dotenv = require 'dotenv'
dotenv.load()

# Data Functions
prepareTables = (client, callback) ->
  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  preInsertQueries = [
    "drop table if exists model_results_new;",
    "create table model_results_new (predictionStartTime timestamp with time zone,
       predictionTime timestamp with time zone, pairId integer, percentile varchar(3),
       predictedSpeed double precision);"
  ]

  async.eachSeries preInsertQueries, issueQuery, (err) ->
    console.log 'fresh model_results table created'
    callback null, client

getFiles = (client, callback) ->
  files = fs.readdirSync process.env.MODEL_RESULTS_PATH
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
    prediction = process.env.MODEL_RESULTS_PATH+'/'+file
    buffer = fs.readFileSync prediction
    zlib.gunzip buffer, (err, data) ->
      console.log "error:" + err if err
      modelResultsQuery = ""
      data = JSON.parse data
      processPredictionData file, data, client, callback

processPredictionData = (file, data, client, callback) ->
  modelResultsQuery = ''
  for pairId of data
    if pairId isnt 'Start'
      for percentile of data[pairId]
        predictionStartTime = new Date(data['Start']+"-05:00")
        predictionStartTimeString = predictionStartTime.toISOString()
        percentileData = data[pairId][percentile]
        if percentileData isnt null
          predictionTime = predictionStartTime
          for predictedSpeed in percentileData
            modelResultsQuery += "insert into model_results_new (predictionStartTime,
              predictionTime, pairId, percentile, predictedSpeed) values
              ('"+predictionStartTimeString+"', '"+predictionTime.toISOString()+"',
              "+pairId+", '"+percentile+"', "+predictedSpeed+");\n"
            predictionTime = new Date(predictionTime.getTime() + 300000)
  client.query modelResultsQuery, (err, result) ->
    if err
      console.log err
    else
      console.log file + " processed"
    callback()

finalizeTables = (client, callback) ->
  console.log 'finalizing import'

  issueQuery = (query, internalCallback) ->
    client.query query, (err, result) ->
      if err
        console.log err
      internalCallback()

  postInsertQueries = [
    'drop table if exists model_results;'
    ,'CREATE INDEX mrpredictionstarttimeidx ON model_results_new USING btree (predictionStartTime);'
    ,'CREATE INDEX mrpredictiontimeidx ON model_results_new USING btree (predictionTime);'
    ,'CREATE INDEX mrpercentileidx ON model_results_new USING btree (percentile);'
    ,'CREATE INDEX mrpairididx ON model_results_new USING btree (pairId);'
    ,'ALTER TABLE model_results_new RENAME TO model_results;'
  ]
  async.eachSeries postInsertQueries, issueQuery, (err) ->
    callback null, client

importStream = (client, callback) ->
  predictionsRaw = ''
  process.stdin.setEncoding 'utf8'
  process.stdin.on 'readable', () ->
    chunk = process.stdin.read()
    if chunk isnt null
      predictionsRaw += chunk

  #process.stdin.on 'end', () ->
  #  predictions = JSON.parse predictionsRaw
  #  processPredictionData predictions, client, callback

main = () ->
  if 'onDisk' is process.argv[2]
    waterfallFunctions = [
      utils.initializeConnection,
      prepareTables,
      getFiles,
      finalizeTables
      utils.terminateConnection
    ]
  else
    waterfallFunctions = [
      utils.initializeConnection,
      importStream,
      utils.terminateConnection
    ]
  async.waterfall waterfallFunctions

main()

# Libraries
fs = require 'fs'
async = require 'async'
zlib = require 'zlib'
utils = require './utils'
dotenv = require 'dotenv'
dotenv.load()

modelResultsTable = process.argv[3]

# Data Functions
prepareTables = (client, callback) ->
  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  preInsertQueries = [
    "drop table if exists "+modelResultsTable+"_new;",
    "create table "+modelResultsTable+"_new (predictionStartTime timestamp with time zone,
       predictionTime timestamp with time zone, pairId integer, percentile varchar(3),
       min smallint, 10 smallint, 25 smallint, 50 smallint, 75 smallint, 90 smallint, max smallint);"
  ]

  async.eachSeries preInsertQueries, issueQuery, (err) ->
    console.log 'fresh '+modelResultsTable+' table created'
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
  predictionStartTime = new Date(data['Start']+"-05:00")
  predictionStartTimeString = predictionStartTime.toISOString()
  percentiles = ['min', '10', '25', '50', '75', '90', 'max']

  # Process Each PairID in the File
  for pairId, pairData of data
    if pairId isnt 'Start'
      predictionTime = predictionStartTime
      predictions = data[pairId]
      predictionsLength = pairData.length
      if predictionsLength isnt 0 or predictions[0] isnt null
        i = 0
        while i < predictionsLength
           
          # Coalesce Prediction Times
          predictionString = ""
          for percentile in percentiles
            prediction = predictions[percentile][i]
            predictionString += prediction + ", "
          
          # Insert Data
          modelResultsQuery += "insert into "+modelResultsTable+"_new (predictionStartTime,
            predictionTime, pairId, min, 10, 25, 50, 75, 90, max) values
            ('"+predictionStartTimeString+"', '"+predictionTime.toISOString()+"',
            "+pairId+", "+ predictionString +");\n"
          predictionTime = new Date(predictionTime.getTime() + 300000)
          i++
  
  # Insert the File Data
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
    'drop table if exists '+modelResultsTable+';'
    ,'CREATE INDEX mrpredictionstarttimeidx ON '+modelResultsTable+'_new USING btree (predictionStartTime);'
    ,'CREATE INDEX mrpredictiontimeidx ON '+modelResultsTable+'_new USING btree (predictionTime);'
    ,'CREATE INDEX mrpercentileidx ON '+modelResultsTable+'_new USING btree (percentile);'
    ,'CREATE INDEX mrpairididx ON '+modelResultsTable+'_new USING btree (pairId);'
    ,'ALTER TABLE '+modelResultsTable+'_new RENAME TO '+modelResultsTable+';'
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

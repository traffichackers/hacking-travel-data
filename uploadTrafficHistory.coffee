# Includes
async = require 'async'
utils = require './utils'
dotenv = require 'dotenv'
dotenv.load()

processor = (client, query, fileName, callback) ->
  console.log 'hi'
  callback null, 'failure', fileName

processCsv = (client, query, fileName, callback) ->
  console.log 'running csv'
  client.query query, (err, result) ->
    console.log 'history received'

    # Generate the CSV string
    historyCsv = ''
    for row in result.rows
      if historyCsv is ''
        keys = Object.keys(row)
        historyCsv = keys.join(',')
      tempRow = []
      for key in keys
        tempRow.push row[key]
      historyCsv += '\n'+tempRow.join(',')
    console.log 'uploading history'
    utils.terminateConnection client, () ->
      callback null, historyCsv, fileName

processJson = (query, fileName, client, callback) ->
  console.log('running json')
  client.query query, (err, result) ->
    if err
      console.log(err)
    else
      console.log 'today data pulled'
      today = {}
      for row in result.rows

        # Initialize objects, if necessary
        if !today[row.pairid]?
          today[row.pairid] = []

        # Populate Data Fields
        today[row.pairid].push Math.round(row.speed)
        if !today.Start
          console.log row.lastupdated.toISOString()
          today.Start = row.lastupdated.toISOString()

      utils.terminateConnection client, () ->
        callback null, today, fileName

main = () ->

  # Create Query
  lookBack = process.argv[2]
  query = "select pairid, lastUpdated::timestamp as lastupdated, speed from history where "
  if lookBack is 'today'
    query += "lastUpdated::date = now()::date order by pairId, lastUpdated"
  else if lookBack.slice(-1) in ['h','d','m','y']
    unitMap = {'h':'hour', 'd':'day', 'm':'month', 'y':'year'}
    lookBackUnits = unitMap[lookBack.slice(-1)]
    lookBackValue = lookBack.substr 0, lookBack.length-1
    query += "lastUpdated > now() - INTERVAL '"+lookBackValue+" "+lookBackUnits+"'" 
  else
    query = "select pairid, lastUpdated::timestamp as lastupdated, speed, from history where lastUpdated::date > " + lookBack

  console.log query

  # Set Output Format
  fileType = process.argv[3]
  if fileType is 'json'
    processor = processJson
  else if fileType is 'csv'
    processor = processCsv
  else
    console.log 'fileType ' + fileType + ' is not recognized, this script will now exit'
    process.exit 1

  # Set File Name and Path
  fileName = process.argv[4]

  # Start the Waterfall
  waterfallFunctions = [
    utils.initializeConnection,
    async.apply(processor, query, fileName), 
    utils.uploadFile
  ]
  async.waterfall(waterfallFunctions)

main()

# Includes
async = require 'async'
utils = require './utils'
dotenv = require 'dotenv'
dotenv.load()

getTodayData = (client, callback) ->
  console.log 'pulling today data from database'
  todayDataQuery = "select pairId, lastUpdated::timestamp, speed from history where lastUpdated::date = now()::date order by pairId, lastUpdated"
  client.query todayDataQuery, (err, result) ->
    if err
      console.log(err)
    else
      console.log 'today data pulled'
      today = {}
      currentPairId = -1
      currentMinute = -1
      for row in result.rows

        # Initialize objects, if necessary
        if !today[row.pairid]?
          today[row.pairid] = []

        # Populate Data Fields
        if currentMinute isnt row.lastupdated.getMinutes() and currentPairId isnt row.pairId
          today[row.pairid].push Math.round(row.speed)
          currentPairId = row.pairid
          currentMinute = row.lastupdated.getMinutes()
        
        if !today.Start
          console.log row.lastupdated.toISOString()
          today.Start = row.lastupdated.toISOString()

      utils.terminateConnection client, () ->
        callback null, today, 'data/today.json'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection
  getTodayData
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

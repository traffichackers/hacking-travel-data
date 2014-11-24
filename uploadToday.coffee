# Includes
fs = require 'fs'
http = require 'http'
async = require 'async'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration

getTodayData = (client, callback) ->
  console.log 'pulling today data from database'
  todayDataQuery = "select pairId, lastUpdated::timestamp, speed from history where lastUpdated::date = now()::date order by pairId, lastUpdated"
  client.query todayDataQuery, (err, result) ->
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
        callback null, today, 'data/today.json'

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection
  getTodayData
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

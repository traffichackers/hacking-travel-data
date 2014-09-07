# Includes
fs = require 'fs'
http = require 'http'
async = require 'async'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration

getTodayData = (current, client, callback) ->
  utils.initializeConnection (err, client) ->
    todayDataQuery = "select pairId, lastUpdated::timestamp::time, stale, travelTime, speed, freeFlow from history where lastUpdated::date = now()::date order by pairId, lastUpdated"
    console.log 'pulling today data from database'

    client.query todayDataQuery, (err, result) ->
      console.log 'today data pulled'
      today = {}

      for row in result.rows

        # Initialize objects, if necessary
        if pairdata[row.pairid]?
          today[row.pairid] = []

          # Delete Objects
          lastUpdated = row.lastupdated.substr(0,5)
          travelTime = Math.round(row.traveltime)
          today [row.pairid].today.push travelTime

      utils.terminateConnection client, () ->
        callback null, current, 'data/today.json'


# Start the Waterfall
waterfallFunctions = [
  getTodayData
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

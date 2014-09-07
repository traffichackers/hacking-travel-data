# Includes
fs = require 'fs'
http = require 'http'
async = require 'async'
utils = require './utils'  # Require
config = require './config.json'  # Server Configuration

getTodayData = (callback) ->
  utils.initializeConnection (err, client) ->
    todayDataQuery = "select pairId, lastUpdated::timestamp::time, stale, travelTime, speed, freeFlow from history where lastUpdated::date = now()::date order by pairId, lastUpdated"
    console.log 'pulling today data from database'

    client.query todayDataQuery, (err, result) ->
      console.log(err)
      console.log 'today data pulled'
      today = {}

      for row in result.rows

        # Initialize objects, if necessary
        if !today[row.pairid]?
          today[row.pairid] = []
       
        lastUpdated = row.lastupdated.substr(0,5)
        travelTime = Math.round(row.traveltime)
        today[row.pairid].push travelTime

      today.Start = lastUpdated 

      utils.terminateConnection client, () ->
        callback null, today, 'data/today.json'


# Start the Waterfall
waterfallFunctions = [
  getTodayData
  utils.uploadFile
]
async.waterfall(waterfallFunctions)

# Library
pg = require 'pg'
async = require 'async'
utils = require './utils'

# Config
config = require './config.json'

createTable = (client, callback) ->
  query = "create table history ( pairId integer, lastUpdated timestamp, stale boolean, travelTime double precision, speed double precision, freeFlow double precision);"
  client.query query, (err, results) ->
    console.log results
    callback null, client

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  createTable, 
  utils.terminateConnection
]
async.waterfall(waterfallFunctions)


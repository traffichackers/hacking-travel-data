# Library
pg = require 'pg'
async = require 'async'
utils = require './utils'

# Config
config = require './config.json'

createHistoryTable = (client, callback) ->

  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  creationQueries = ["create table history ( pairId integer, lastUpdated timestamp, stale boolean, travelTime double precision, speed double precision, freeFlow double precision);"
  ,'CREATE INDEX lastupdatedidx ON history USING btree (lastupdated);'
  ,'CREATE INDEX pairididx ON history USING btree (pairid);']

  async.eachSeries creationQueries, issueQuery, (err) ->
    callback(null, client)

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  createTable,
  utils.terminateConnection
]
async.waterfall(waterfallFunctions)

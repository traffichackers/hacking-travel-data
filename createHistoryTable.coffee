# Library
async = require 'async'
utils = require './utils'

createHistoryTable = (client, callback) ->

  issueQuery = (query, internalCallback) ->
    client.query query, (err, results) ->
      internalCallback(null, results)

  creationQueries = ["create table history ( pairId integer, lastUpdated timestamp with time zone, stale boolean, travelTime double precision, speed double precision, freeFlow double precision);"
  ,'create index lastupdatedidx on history using btree (lastupdated);'
  ,'create index pairididx on history using btree (pairid);']

  async.eachSeries creationQueries, issueQuery, (err) ->
    callback(null, client)

# Start the Waterfall
waterfallFunctions = [
  utils.initializeConnection,
  createHistoryTable,
  utils.terminateConnection
]
async.waterfall(waterfallFunctions)

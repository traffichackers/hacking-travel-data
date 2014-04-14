# Library
fs = require 'fs'
xml2js = require 'xml2js'
ftp = require 'ftp'
http = require 'http'
pg = require 'pg' 
async = require 'async'
csv = require 'csv'

# Config
config = require './config.json'

# Connection
initializeConnection = (callback) ->
  console.log 'initializing connnection'
  connectionString = config.postgresConnectionString
  client = new pg.Client(config.connectionOptions)
  client.connect (err) ->
    if err
      return console.error 'could not connect to postgres', err
    else
      console.log 'connection initialized'
      callback null, client



# Start the Waterfall
waterfallFunctions = [
  initializeConnection,
  issueAllQuery,
  issueDowQuery
]
async.waterfall(waterfallFunctions)

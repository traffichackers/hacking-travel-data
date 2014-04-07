# Library
fs = require 'fs'
xml2js = require 'xml2js'
http = require 'http'
csv = require 'csv'

# Utilities
getDirs = (rootDir) ->
  files = fs.readdirSync(rootDir)
  dirs = []

  for file in files
    if file[0] != '.'
      filePath = "#{rootDir}/#{file}"
      stat = fs.statSync(filePath)
      if (stat.isDirectory())
        dirs.push(file)

  return dirs

# CSV Generation
attributes = {'Stale':'stale','TravelTime':'travelTime','Speed':'speed','FreeFlow':'freeFlow'}
extractMultipleFileData = () ->
  fd = fs.openSync 'data.csv', 'a', undefined
  metaData = []
  directories = getDirs __dirname+'/xml'
  for directory in directories
    files = fs.readdirSync __dirname+'/xml/'+directory
    for file, i in files
      if file.slice(-4) is '.xml'
        data = fs.readFileSync __dirname+'/xml/'+directory+'/'+file, 'ascii'
        if data.slice(0,5) == '<?xml'
          parser.parseString data, (err, result) ->
            travelData = result.btdata?.TRAVELDATA[0]
            lastUpdated = travelData?.LastUpdated[0]
            pairData = travelData?.PAIRDATA
            for pair in pairData
              travelDataString = ''
              travelDataString += lastUpdated
              pairId = pair['PairID'][0]
              travelDataString += ','+pairId
              for mDotName, internalName of attributes
                travelDataString += ','+pair[mDotName][0]
              travelDataString += '\n'
              fs.writeSync fd, travelDataString, undefined, undefined
      if i%10 is 0
        console.log i + " files written"
  return true

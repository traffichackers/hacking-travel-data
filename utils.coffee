module.exports =
  parseMassDotXml:  (data, parser, callback) ->
    if data.slice(0,5) == '<?xml'
      parser.parseString data, (err, result) ->
        travelData = result.btdata?.TRAVELDATA[0]
        lastUpdated = travelData?.LastUpdated[0]
        pairData = travelData?.PAIRDATA
        callback({'lastUpdated':lastUpdated, 'pairData':pairData})

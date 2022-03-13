
const AWS = require('aws-sdk');
const timeseries = require("./timeseries-analytics");
// Configuring AWS SDK
AWS.config.update({ region: "us-east-1" });
async function callServices() {


    queryClient = new AWS.TimestreamQuery();
    var https = require('https');
    var agent = new https.Agent({
        maxSockets: 5000
      });
      writeClient = new AWS.TimestreamWrite({
        maxRetries: 10,
        httpOptions: {
          timeout: 20000,
          agent: agent
        }
      });
    
    var results = {}

  
    results = await timeseries.getSignificantEvents(queryClient, "wind", "1h", "365d", "20")
    console.log(results)
    var writeresults = await timeseries.writeEventRecords(writeClient, results.dataset, "threshold", "high", results.metadata.measure_name)
    console.log(writeresults)
}

callServices();
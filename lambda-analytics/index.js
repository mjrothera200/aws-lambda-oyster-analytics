// dependencies
const AWS = require('aws-sdk');
const util = require('util');
const timeseries = require("./timeseries-analytics");

// Modeled after:
// https://docs.aws.amazon.com/lambda/latest/dg/with-s3-tutorial.html


// get reference to S3 client
const s3 = new AWS.S3();

exports.handler = async (event, context, callback) => {

    // Setup timestream
    /**
    * Recommended Timestream write client SDK configuration:
    *  - Set SDK retry count to 10.
    *  - Use SDK DEFAULT_BACKOFF_STRATEGY
    *  - Set RequestTimeout to 20 seconds .
    *  - Set max connections to 5000 or higher.
    */

    console.log('Received event:', JSON.stringify(event, null, 2));
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

    // Threshold event for sustained wind over 20 mph
    results = await timeseries.getSignificantEvents(queryClient, "wind", "1h", "30d", "20")
    console.log(results)
    var writeresults = await timeseries.writeEventRecords(writeClient, results.dataset, "threshold", "high", results.metadata.measure_name)
    console.log(writeresults)
    
    // Rate of change event for a rain gusher
    results = await timeseries.getRateOfChangeEvents(queryClient, "rain", "1h", "30d", "15")
    console.log(results)
    var writeresults = await timeseries.writeEventRecords(writeClient, results.dataset, "rateofchange", "high", results.metadata.measure_name)
    console.log(writeresults)

    return results;

};

const constants = require('./constants');

const HOSTNAME = "host1";

const measure_metadata = {
    watertemp: {
        database: constants.DATABASE_NAME,
        table: constants.TEMP_LOGGER_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "tempf",
        measure_value: "measure_value::double",
        yunits: "º",
        ytitle: "Water Temperature",
        lowthreshold: 38,
        highthreshold: 100,
        ydomainlow: 0,
        ydomainhigh: 120
    },
    temp: {
        database: constants.DATABASE_NAME,
        table: constants.WEATHER_DATA_TABLE_NAME,
        slicevalue: "source",
        measure_name: "temp",
        measure_value: "measure_value::varchar",
        yunits: "º",
        ytitle: "Outside Temperature",
        lowthreshold: 38,
        highthreshold: 100,
        ydomainlow: 0,
        ydomainhigh: 120
    },
    wind: {
        database: constants.DATABASE_NAME,
        table: constants.WEATHER_DATA_TABLE_NAME,
        slicevalue: "source",
        measure_name: "wind",
        measure_value: "measure_value::varchar",
        yunits: "mph",
        ytitle: "Wind Speed",
        lowthreshold: 0,
        highthreshold: 50,
        ydomainlow: 0,
        ydomainhigh: 90
    },
    waterlight: {
        database: constants.DATABASE_NAME,
        table: constants.TEMP_LOGGER_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "lumensft2",
        measure_value: "measure_value::double",
        yunits: "lumens ft2",
        ytitle: "Water Light",
        lowthreshold: 0,
        highthreshold: 800,
        ydomainlow: 0,
        ydomainhigh: 1000
    },
    watertemprt: {
        database: constants.DATABASE_NAME,
        table: constants.WATER_DATA_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "tempf",
        measure_value: "measure_value::double",
        yunits: "º",
        ytitle: "Water Temperature (Real-Time)",
        lowthreshold: 38,
        highthreshold: 100,
        ydomainlow: 0,
        ydomainhigh: 120
    },
    tds: {
        database: constants.DATABASE_NAME,
        table: constants.WATER_DATA_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "tds",
        measure_value: "measure_value::double",
        yunits: "mg/L",
        ytitle: "Total Dissolved Solids",
        lowthreshold: 10000,
        highthreshold: 40000,
        ydomainlow: 1000,
        ydomainhigh: 50000
    },
    salinity: {
        database: constants.DATABASE_NAME,
        table: constants.WATER_DATA_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "salinity",
        measure_value: "measure_value::double",
        yunits: "mg/L",
        ytitle: "Salinity",
        lowthreshold: 0,
        highthreshold: 18000,
        ydomainlow: 500,  // Brackish between 500 and 5000, sea water at 19,400
        ydomainhigh: 20000
    },
    ec: {
        database: constants.DATABASE_NAME,
        table: constants.WATER_DATA_TABLE_NAME,
        slicevalue: "sensorid",
        measure_name: "ec",
        measure_value: "measure_value::double",
        yunits: "us/cm",
        ytitle: "Conductivity",
        lowthreshold: 0,
        highthreshold: 18000,
        ydomainlow: 500,  // Brackish between 500 and 5000, sea water at 19,400
        ydomainhigh: 20000
    },
}

async function getMeasures() {

    return Object.keys(measure_metadata)

}

// queryclient:  the AWS timestream client
// measure_name:  corresponds to the measure name in the table above
// bintime:  the window interval (i.e "1h" for one hour)
// queryduration:  the overall timespan of analytics (i.e "365d" for the last 365 days)
// threshold:  the average value that will trigger the event to be raised (i.e over 20 mph for wind - '20')

async function getSignificantEvents(queryClient, measure_name, bintime, queryduration, threshold) {

    // get the actual details from the metadata
    const measure = measure_metadata[measure_name]
    console.log(measure)

    const query = `with binnedData AS ( SELECT BIN(time, ${bintime}) AS binned_timestamp, ${measure.slicevalue}, ROUND(AVG(CAST(${measure.measure_value} as DOUBLE)),2) AS avg_value, LAG(ROUND(AVG(CAST(${measure.measure_value} as DOUBLE)),2), 1) OVER ( PARTITION BY ${measure.slicevalue} ORDER BY BIN(time, ${bintime}) ) AS prev_avg_value FROM "${measure.database}"."${measure.table}" WHERE measure_name = '${measure.measure_name}' and time between ago(${queryduration}) and now() GROUP BY source, BIN(time, 1h) ORDER BY binned_timestamp ASC), cleanedData AS ( select binned_timestamp, ${measure.slicevalue}, avg_value, prev_avg_value, LAG(binned_timestamp, 1) OVER ( PARTITION BY ${measure.slicevalue} ORDER BY binned_timestamp ) AS prev_binned_timestamp from binnedData where avg_value > ${threshold} and prev_avg_value > ${threshold} order by binned_timestamp ), summarized AS ( select binned_timestamp,avg_value,binned_timestamp - prev_binned_timestamp as duration from cleanedData )
select to_milliseconds(BIN(binned_timestamp, 1d)) AS x, avg(avg_value) as y, sum(duration) as duration
        from summarized
            GROUP BY BIN(binned_timestamp, 1d)
        order by x ASC`

    console.log(query)

    const queries = [query];

    var parsedRows = []
    for (let i = 0; i < queries.length; i++) {
        console.log(`Running query ${i + 1} : ${queries[i]}`);
        parsedRows = await getAllRows(queryClient, queries[i], null);
    }
    const results = convertSignificantEventRows(parsedRows)
    results["metadata"] = measure
    return results
}

function convertSignificantEventRows(parsedRows) {
    var results = {}
    var dataset = []
    var hints = []
    var max = { x: 0, y: 99999 }
    var min = { x: 0, y: -99999 }
    if (parsedRows) {
        parsedRows.forEach(function (row) {
            const splits = row.split(',')
            var entry = {}
            var sensorname = ""
            splits.forEach(function (field) {
                const fieldSplit = field.split('=')
                const field_name = fieldSplit[0].trim()
                const field_value = fieldSplit[1].trim()
                if (field_name === 'x') {
                    entry.x = parseInt(field_value)
                } else if (field_name === 'y') {
                    entry.y = parseFloat(field_value)
                } else if (field_name === 'duration') {
                    entry.duration = parseFloat(field_value)
                }
            });
            if (entry.y < max.y) {
                max = entry
            }
            if (entry.y > min.y) {
                min = entry
            }
            dataset.push(entry);
        }
        );
    }
    hints.push({ type: "max", max })
    hints.push({ type: "min", min })
    results["dataset"] = dataset
    results["hints"] = hints
    return results;
}



async function getAllRows(queryClient, query, nextToken = undefined) {
    let response;
    try {
        response = await queryClient.query(params = {
            QueryString: query,
            NextToken: nextToken,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    var parsedRows = parseQueryResult(response);
    if (response.NextToken) {
        parsedRows.concat(await getAllRows(query, response.NextToken));
    }
    return parsedRows
}

function parseQueryResult(response) {
    const queryStatus = response.QueryStatus;
    //console.log("Current query status: " + JSON.stringify(queryStatus));

    const columnInfo = response.ColumnInfo;
    const rows = response.Rows;

    //console.log("Metadata: " + JSON.stringify(columnInfo));
    //console.log("Data: ");

    var parsedQueryRows = []
    rows.forEach(function (row) {
        const entry = parseRow(columnInfo, row)
        parsedQueryRows.push(entry);
    });
    return parsedQueryRows
}

function parseRow(columnInfo, row) {
    const data = row.Data;
    const rowOutput = [];

    var i;
    for (i = 0; i < data.length; i++) {
        info = columnInfo[i];
        datum = data[i];
        rowOutput.push(parseDatum(info, datum));
    }

    return `${rowOutput.join(", ")}`
}

function parseDatum(info, datum) {
    if (datum.NullValue != null && datum.NullValue === true) {
        return `${info.Name}=NULL`;
    }

    const columnType = info.Type;

    // If the column is of TimeSeries Type
    if (columnType.TimeSeriesMeasureValueColumnInfo != null) {
        return parseTimeSeries(info, datum);
    }
    // If the column is of Array Type
    else if (columnType.ArrayColumnInfo != null) {
        const arrayValues = datum.ArrayValue;
        return `${info.Name}=${parseArray(info.Type.ArrayColumnInfo, arrayValues)}`;
    }
    // If the column is of Row Type
    else if (columnType.RowColumnInfo != null) {
        const rowColumnInfo = info.Type.RowColumnInfo;
        const rowValues = datum.RowValue;
        return parseRow(rowColumnInfo, rowValues);
    }
    // If the column is of Scalar Type
    else {
        return parseScalarType(info, datum);
    }
}

function parseTimeSeries(info, datum) {
    const timeSeriesOutput = [];
    datum.TimeSeriesValue.forEach(function (dataPoint) {
        timeSeriesOutput.push(`{time=${dataPoint.getTime()}, value=${parseDatum(info.Type.TimeSeriesMeasureValueColumnInfo, dataPoint.Value)}}`)
    });

    return `[${timeSeriesOutput.join(", ")}]`
}

function parseScalarType(info, datum) {
    return parseColumnName(info) + datum.ScalarValue;
}

function parseColumnName(info) {
    return info.Name == null ? "" : `${info.Name}=`;
}

function parseArray(arrayColumnInfo, arrayValues) {
    const arrayOutput = [];
    arrayValues.forEach(function (datum) {
        arrayOutput.push(parseDatum(arrayColumnInfo, datum));
    });
    return `[${arrayOutput.join(", ")}]`
}

// Write

async function writeEventRecords(writeClient, events, eventType , eventClassification, measure_name) {
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    var records = []
    var counter = 0;
    var processed = 0;
    const promises = [];

    var start_time = Date.now()


    while (counter < events.length) {

        const dimensions = [
            { 'Name': 'eventType', 'Value': eventType },
            { 'Name': 'eventClassification', 'Value': eventClassification }

        ];
        const recordTime = currentTime - counter * 50;
        let version = Date.now();
        const timestamp = new Date(events[counter].x)
        var value = 0.0
        value = (events[counter].y.toString().trim().length > 0) ? parseFloat(events[counter].y.toString()) : 0.0;
        
        const record1 = {
            'Dimensions': dimensions,
            'MeasureName': measure_name,
            'MeasureValue': value.toString(),
            'MeasureValueType': 'DOUBLE',
            'Time': timestamp.getTime().toString(),
            'Version': version
        };
        //console.log(record1)

        records.push(record1);

        // 
        processed++;

        counter++;

        if (records.length === 100) {
            promises.push(submitBatch(records, counter, writeClient));
            records = [];
        }


    }


    if (records.length !== 0) {
        promises.push(submitBatch(records, counter, writeClient));
    }

    await Promise.all(promises).then(() => {

    });

    var end_time = Date.now()
    var processing_time = (end_time - start_time)

    console.log("Complete.")
    console.log(`Ingested ${counter} records.  Processed ${processed} records.`);
    var results = {
        ingested: counter,
        processed: processed,
        startTime: new Date(start_time).toString(),
        endTime: new Date(end_time).toString(),
        processingTime: processing_time,
    }
    return results;

}

function submitBatch(records, counter, writeClient) {
    const params = {
        DatabaseName: constants.DATABASE_NAME,
        TableName: constants.EVENTS_DATA_TABLE_NAME,
        Records: records
    };

    var promise = writeClient.writeRecords(params).promise();

    return promise.then(
        (data) => {
            console.log(`Processed ${counter} records.`);
        },
        (err) => {
            console.log("Error writing records:", err);
        }
    );
}


module.exports = { getMeasures, getSignificantEvents, writeEventRecords };
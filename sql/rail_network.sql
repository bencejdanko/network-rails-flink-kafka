CREATE TABLE rail_network (
    updateOrigin STRING,
    requestSource STRING,
    requestID STRING,
    train ROW<rid STRING, uid STRING, ssd STRING>,
    stationUpdates ARRAY<ROW<
        stationCode STRING, 
        workingTimePlanned STRING, 
        estimatedTime STRING, 
        platform STRING, 
        platformConfirmed BOOLEAN
    >>
) WITH (
    'connector' = 'kafka',
    'topic' = 'rail_network',
    'properties.bootstrap.servers' = 'kafka:9093',
    'properties.group.id' = 'flink-consumer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

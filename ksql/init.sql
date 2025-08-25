-- Create the input stream from sensor-data topic
CREATE STREAM sensor_stream (
    tag_id STRING,
    timestamp STRING,
    value DOUBLE
) WITH (
    KAFKA_TOPIC='sensor-data',
    VALUE_FORMAT='JSON',
    TIMESTAMP='timestamp'
);

-- Create a table to compute 1-minute average per tag
CREATE TABLE aggregated_table
WITH (KAFKA_TOPIC='aggregated-data') AS
SELECT
    tag_id,
    WINDOWSTART AS window_start,
    WINDOWEND AS window_end,
    AVG(value) AS avg_value
FROM sensor_stream
WINDOW TUMBLING (SIZE 1 MINUTE)
GROUP BY tag_id
EMIT CHANGES;

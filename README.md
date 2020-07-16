# Flume Append HBase Serializer

## Build
mvn package

## Install
Copy target/flume-appendhbaseevent-serializer-1.0.0.jar in Flume' classpath

## Configure
hbaseagent.sinks.prices.serializer = com.marketconnect.flume.serializer.AppendHbaseEventSerializer
hbaseagent.sinks.prices.serializer.colName = log

It will append the data to column log
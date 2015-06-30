# csv-loader
Command line tool for pushing CSV data into traffic ingest via HTTP or Kinesis

## Usage Instructions

Requires Java 1.8+

1. Download latest release build from the releases page or build from source
2. Install and launch traffic-engine-app (https://github.com/opentraffic/traffic-engine-app)
3. Format vehicle location data as CSV with the format "unique_vehicle_id,utc_timestamp,lat,lon"
4. Run app 
```
java -jar target/csv-loader.jar -f [path to CSV file] -u http://localhost:4567/locationUpdate
```

## Build Instructions

1. git clone
2. mvn package
3. Built packages in target/csv-loader.jar 

register datafu-1.2.0.jar;
define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.1','0.2','0.3','0.4','0.5','0.6','0.7','0.8','0.9','1.0');
data = load 'clean_data' using PigStorage() as (rank: long, medallion: chararray, hack_license: chararray, vendor_id: chararray, rate_code: chararray, store_and_fwd_flag: chararray, pickup_datetime: chararray, dropoff_datetime: chararray, passenger_count: int, trip_time_in_secs: long, trip_distance: double, pickup_longitude: chararray, pickup_latitude: chararray, dropoff_longitude: chararray, dropoff_latitude: chararray);
quantiles = FOREACH (GROUP data ALL) GENERATE Quantile(data.trip_distance);
STORE quantiles INTO 'trip_distance_quantiles' USING PigStorage();
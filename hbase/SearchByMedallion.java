package hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class SearchByMedallion {
	
private static Connection connection;
	
	public static void getConnection() throws IOException{
		Configuration config = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(config);
	}
	
	public static void scanData() throws IOException{
		Table table = connection.getTable(TableName.valueOf("trip_data_10"));
		Scan s = new Scan();
		s.setRowPrefixFilter(Bytes.toBytes("2013000360"));
		s.addFamily(Bytes.toBytes("details"));
		ResultScanner scanner = table.getScanner(s);
		for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
			String rowKey = new String(rr.getRow(), "UTF-8");
			String medallion = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("medallion")), "UTF-8");
			String hack_license = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("hack_license")), "UTF-8");
			String vendor_id = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("vendor_id")), "UTF-8");
			String rate_code = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("rate_code")), "UTF-8");
			String passenger_count = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("passenger_count")), "UTF-8");
			String trip_time_in_secs = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("trip_time_in_secs")), "UTF-8");
			String trip_distance = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("trip_distance")), "UTF-8");
			String pickup_longitude = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("pickup_longitude")), "UTF-8");
			String pickup_latitude = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("pickup_latitude")), "UTF-8");
			String dropoff_longitude = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("dropoff_longitude")), "UTF-8");
			String dropoff_latitude = new String(rr.getValue(Bytes.toBytes("details"), Bytes.toBytes("dropoff_latitude")), "UTF-8");
			System.out.println( rowKey+","
								+medallion + ","
								+hack_license+","
								+vendor_id+","
								+rate_code+","
								+passenger_count+","
								+trip_time_in_secs+","
								+trip_distance+","
								+pickup_longitude+","
								+pickup_latitude+","
								+dropoff_longitude+","
								+dropoff_latitude);
		  }
		  scanner.close();
		  table.close();
	}


}

package hbase;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseUpload {
	
	private static Connection connection;
	
	public static void getConnection() throws IOException{
		Configuration config = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(config);
	}
	
	public static void createTable() throws IOException{
		Admin  admin = connection.getAdmin();
		HTableDescriptor newtable = new HTableDescriptor(TableName.valueOf("trip_data"));
		newtable.addFamily(new HColumnDescriptor(Bytes.toBytes("details")));
		if (!admin.tableExists(TableName.valueOf("trip_data"))) admin.createTable(newtable);
		admin.close();
	}
	
	public static void uploadData() throws IOException, ParseException{
		BufferedReader br = new BufferedReader(new FileReader("/Users/Vaibhav/trip_data.csv"));
		Table table = connection.getTable(TableName.valueOf("trip_data"));
		for(String line; (line = br.readLine()) != null; ) {
			String[] tokens = line.split(",");
			if (tokens.length == 19){
				Put p = new Put(Bytes.toBytes(tokens[0] + " " + tokens[17]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("medallion"), Bytes.toBytes(tokens[0]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("hack_license"),Bytes.toBytes(tokens[1]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("vendor_id"), Bytes.toBytes(tokens[2]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("rate_code"),Bytes.toBytes(tokens[3]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_datetime"),Bytes.toBytes(tokens[4]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_datetime"), Bytes.toBytes(tokens[5]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("passenger_count"),Bytes.toBytes(tokens[6]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("trip_time_in_secs"), Bytes.toBytes(tokens[7]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("trip_distance"),Bytes.toBytes(tokens[8]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_longitude"), Bytes.toBytes(tokens[9]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_latitude"),Bytes.toBytes(tokens[10]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_longitude"), Bytes.toBytes(tokens[11]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_latitude"),Bytes.toBytes(tokens[12]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_date"),Bytes.toBytes(tokens[13]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_time"),Bytes.toBytes(tokens[14]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_date"), Bytes.toBytes(tokens[15]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_time"), Bytes.toBytes(tokens[16]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("pickup_timestamp"),Bytes.toBytes(tokens[17]));
				p.addColumn(Bytes.toBytes("details"), Bytes.toBytes("dropoff_timestamp"), Bytes.toBytes(tokens[18]));

				table.put(p);				
			}
		}		
		table.close();
		br.close();
	}

	public static void main(String[] args) throws IOException, ParseException {
		getConnection();
		createTable();
		uploadData();
		connection.close();
	}

}

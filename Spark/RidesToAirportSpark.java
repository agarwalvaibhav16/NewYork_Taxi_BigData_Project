package bigData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RidesToAirportSpark {
	
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
	    String logFile = "/Users/Vaibhav/trip_data.csv";
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Rides To Airport");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile);
	    
	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { 
	    	  String[] tokens = s.split(",");
	        	return tokens[3].contains("2");
	   }
	    }).count();
	    System.out.println(numAs);
	    sc.close();
	  }


}

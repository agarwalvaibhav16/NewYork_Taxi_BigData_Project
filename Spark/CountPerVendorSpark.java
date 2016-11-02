package bigData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class CountPerVendorSpark {
	
	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
	    String logFile = "/Users/Vaibhav/trip_data.csv";
	    SparkConf conf = new SparkConf().setMaster("local").setAppName("Count By Vendor");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	    JavaRDD<String> logData = sc.textFile(logFile);

	    long numAs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("VTS"); }
	    }).count();

	    long numBs = logData.filter(new Function<String, Boolean>() {
	      public Boolean call(String s) { return s.contains("CMT"); }
	    }).count();

	    System.out.println("VTS: " + numAs + ", CMT: " + numBs);
	    
	    sc.close();
	  }


}

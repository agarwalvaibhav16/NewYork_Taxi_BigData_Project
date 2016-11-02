package bigData;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.WeightedVectorWritable;
import org.apache.mahout.clustering.kmeans.Cluster;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.common.distance.EuclideanDistanceMeasure;
import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class KMeansClustering {

	public static void main(String args[]) throws Exception {

		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		SequenceFile.Writer writer1 = new SequenceFile.Writer(fs, conf, 
				new Path("testdata/points/file"), LongWritable.class, VectorWritable.class);
		long recNum = 0;
		VectorWritable vec1 = new VectorWritable();
		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, 
				new Path("testdata/clusters/part-00000"), Text.class, Cluster.class);
		BufferedReader br = new BufferedReader(new FileReader("/Users/Vaibhav/trip_data.csv"));
		for(String line; (line = br.readLine()) != null; ) {
			String[] tokens = line.split(",");
	    	  double[] fr = {Double.parseDouble(tokens[7]), Double.parseDouble(tokens[8])};
	    	  Vector vec = new RandomAccessSparseVector(2);
			vec.assign(fr);
			vec1.set(vec);
			if(recNum < 10){
				int newInt = (int) recNum;
				Cluster cluster = new Cluster(vec, newInt, new EuclideanDistanceMeasure());
				writer.append(new Text(cluster.getIdentifier()), cluster);
			}
			writer1.append(new LongWritable(recNum++), vec1);
		}
		br.close();
		writer1.close();
		writer.close();
		
		KMeansDriver.run(conf, new Path("testdata/points"), 
				new Path("testdata/clusters"), new Path("output"), 
				new EuclideanDistanceMeasure(), 0.001, 10, true, false);
		
		for(int i =0;i<16;i++){
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, 
				new Path("output/" + Cluster.CLUSTERED_POINTS_DIR + "/part-m-0000"+i), conf);
		IntWritable key = new IntWritable();
		WeightedVectorWritable value = new WeightedVectorWritable();
		File file = new File("clusters"+i+".txt");
		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		BufferedWriter out = new BufferedWriter(fw);
		while (reader.next(key, value)) {
			out.write(value.toString() +"," +key.toString());
			out.newLine();
		}
		out.close();
		reader.close();
		}
	}


}

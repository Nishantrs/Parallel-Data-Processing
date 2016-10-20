package mapreduceavgcal.mravgcal;

import java.io.IOException;
//import java.util.StringTokenizer;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MapperReducerStationAvg {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "Mapper Reducer");
		job.setJarByClass(MapperReducerStationAvg.class);
		job.setMapperClass(CsvFileReaderMapper.class);
		job.setReducerClass(StationTempAvgReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationInstance.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job,
				new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


//Reducer Class which outputs Text as key containing required details and NullWritable as value
//Input key is Text and values are StationInstance
class StationTempAvgReducer extends Reducer<Text,StationInstance,Text,NullWritable> {

	private NullWritable result = NullWritable.get();

	// Reduce function which emits accepts StationId and list of StationInstance objects as input
	@Override
	public void reduce(Text key, Iterable<StationInstance> values, Context context) throws IOException, InterruptedException {

		// accumulating structure for calculating Average Maximum temperature and Minimum temperature  
		double accTmin = 0;
		double accTmax = 0;
		int countTmax = 0;
		int countTmin = 0;
		String resultKey = "";


		for (StationInstance station : values) {

			// If the received object contains Tmax value then storing it accumulated sum accTmax and updating counter
			if(station.isTmax()){
				accTmax += station.getTmax();
				countTmax++;
			}else{
				// If the received object contains Tmin value then storing it accumulated sum accTmax and updating counter
				accTmin += station.getTmin();
				countTmin++;
			}
		}

		// Handling exceptional cases in order to avoid NaN as output value
		if(countTmin == 0){

			resultKey = key.toString() + ", " + "Does not exist" + ", " + (accTmax/countTmax);

		} else if(countTmax == 0){

			resultKey = key.toString() + ", " + (accTmin/countTmin) + ", " + "Does not exist";

		}else{

			resultKey = key.toString() + ", " + (accTmin/countTmin) + ", " + (accTmax/countTmax);
		}

		// Emit the corresponding key-value pair
		context.write(new Text(resultKey), result);
	}
}


// Mapper Class
class CsvFileReaderMapper extends Mapper<Object, Text, Text, StationInstance> {

	private Text word;

	// Map function which emits StationId as key and customized StationInstance class as value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Split the input based on the records in each line.
		String[] lines = value.toString().split("\n");


		for (String line : lines) {

			// Consider only those records which contains the following pattern
			if(line.contains("TMAX") || line.contains("TMIN")) {

				String[] recordComponents = line.split(",");
				word = new Text();

				if(recordComponents[2].equals("TMAX")){

					// Setting up the key and extracting the maximum temperature value from the record
					word.set(recordComponents[0]);
					double temp = Double.parseDouble(recordComponents[3]);

					// Emit the corresponding key-value pair 
					context.write(word, new StationInstance(temp,0,true));

				}else if(recordComponents[2].equals("TMIN")){

					// Setting up the key and extracting the minimum temperature value from the record
					word.set(recordComponents[0]);
					double temp = Double.parseDouble(recordComponents[3]);

					// Emit the corresponding key-value pair
					context.write(word, new StationInstance(0,temp,false));

				}
			}
		}

		//      Another approach of reading data from the input
		//
		//		String line = value.toString();
		//		
		//		if(line.contains("TMAX")){
		//			String[] recordComponents = line.split(",");
		//			word = new Text();
		//			word.set(recordComponents[0]);
		//			double temp = Double.parseDouble(recordComponents[3]);
		//			context.write(word, new StationInstance(temp,0,true));
		//			
		//		}
		//		
		//		if(line.contains("TMIN")){
		//			String[] recordComponents = line.split(",");
		//			word = new Text();
		//			word.set(recordComponents[0]);
		//			double temp = Double.parseDouble(recordComponents[3]);
		//			context.write(word, new StationInstance(0,temp,false));
		//			
		//		}
	}
}


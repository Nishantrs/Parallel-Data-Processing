package mapcombreduceavgcal.mrcavgcal;


import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MapCombinerReduce {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}


		Job job = new Job(conf,"Mapper Combiner Reducer");
		job.setJarByClass(MapCombinerReduce.class);
		job.setMapperClass(CsvFileReaderMapper.class);
		job.setCombinerClass(StationTempAvgCombiner.class);
		job.setReducerClass(StationTempAvgReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(StationInstanceInCombine.class);
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
//Input key is Text and values are StationInstanceInCombine
class StationTempAvgReducer extends Reducer<Text,StationInstanceInCombine,Text,NullWritable> {

	private NullWritable result = NullWritable.get();


	@Override
	public void reduce(Text key, Iterable<StationInstanceInCombine> values, Context context) throws IOException, InterruptedException {

		double accTmin = 0;
		double accTmax = 0;
		int countTmax = 0;
		int countTmin = 0;
		String resultKey = "";


		// Ensuring accumulation of temperature and count for a particular key i.e.StationID
		for (StationInstanceInCombine station : values) {

			accTmax += station.getAccTmax();
			countTmax += station.getCountTmax(); 

			accTmin += station.getAccTmin();
			countTmin += station.getCountTmin(); 
		}

		// Handling exceptional cases
		if(countTmin == 0){

			resultKey = key.toString() + ", " + "Does not exist" + ", " + (accTmax/countTmax);

		} else if(countTmax == 0){

			resultKey = key.toString() + ", " + (accTmin/countTmin) + ", " + "Does not exist";

		}else{

			resultKey = key.toString() + ", " + (accTmin/countTmin) + ", " + (accTmax/countTmax);
		}

		context.write(new Text(resultKey), result);
	}
}


// Combiner Class
class StationTempAvgCombiner extends Reducer<Text,StationInstanceInCombine,Text,StationInstanceInCombine> {

	@Override
	public void reduce(Text key, Iterable<StationInstanceInCombine> values, Context context) throws IOException, InterruptedException {

		double accTmin = 0;
		double accTmax = 0;
		int countTmax = 0;
		int countTmin = 0;


		// Ensuring accumulation of temperature and count for a particular key i.e.StationID
		for (StationInstanceInCombine station : values) {

			accTmax += station.getAccTmax();
			countTmax += station.getCountTmax(); 

			accTmin += station.getAccTmin();
			countTmin += station.getCountTmin(); 
		}
		
		StationInstanceInCombine newStation = new StationInstanceInCombine(accTmax, accTmin, countTmax, countTmin);

		//Emit exactly the same type of key-value pair as it input type
		context.write(key, newStation);
	}
}

//Mapper Class
class CsvFileReaderMapper extends Mapper<Object, Text, Text, StationInstanceInCombine> {


	// Map function which emits StationId as key and customized StationInstanceInCombine class as value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		// Split the input based on the records in each line.
		String[] lines = value.toString().split("\n");

		for (String line : lines) {

			// Consider only those records which contains the following pattern
			if (line.contains("TMAX") || line.contains("TMIN")) {

				String[] recordComponents = line.split(",");

				if(recordComponents[2].equals("TMAX")) {

					String stationID = recordComponents[0];

					// Setting up the key and extracting the maximum temperature value from the record and creating value
					// Emit the corresponding key-value pair 
						context.write(new Text(stationID), new StationInstanceInCombine(Double.parseDouble(recordComponents[3]),0.0,1,0));
					
				}

				if(recordComponents[2].equals("TMIN")){

					String stationID = recordComponents[0];

					// Setting up the key and extracting the minimum temperature value from the record and creating value
					// Emit the corresponding key-value pair 
					context.write(new Text(stationID), new StationInstanceInCombine(0.0,Double.parseDouble(recordComponents[3]),0,1));

				}
			}
		}
	}
}


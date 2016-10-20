package mapreducesecavgcal.mrssavgcal;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class SecondarySortMR {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: hadoop jar This.jar <in> [<in>...] <out>");
			System.exit(2);
		}

		Job job = new Job(conf,"Secondary Sorting");
		job.setJarByClass(SecondarySortMR.class);
		job.setMapperClass(CsvFileReaderMapper.class);
		job.setReducerClass(StationTempAvgReducer.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setMapOutputKeyClass(CompositeKey.class);
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

// This comparator helps in final grouping at reducer input based on StationId
class GroupComparator extends WritableComparator {

	protected GroupComparator() {

		super(CompositeKey.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		return key1.getStationID().compareTo(key2.getStationID());

	}
}


// This comparator helps in sorting first based on stationID and then on year when the 
// data was recorded
class KeyComparator extends WritableComparator {

	protected KeyComparator() {

		super(CompositeKey.class, true);

	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		int cmp = key1.getStationID().compareTo(key2.getStationID());

		if (cmp != 0) {

			return cmp;
		}

		return key1.getYear().compareTo(key2.getYear());
	}

}

//Reducer Class which outputs Text as key containing required details and NullWritable as value
//Input key is CompositeKey and values are StationInstanceInCombine
class StationTempAvgReducer extends Reducer<CompositeKey,StationInstanceInCombine,Text,NullWritable> {

	private NullWritable result = NullWritable.get();


	@Override
	public void reduce(CompositeKey key, Iterable<StationInstanceInCombine> values, Context context) throws IOException, InterruptedException {

		double accTmin = 0;
		double accTmax = 0;
		int countTmax = 0;
		int countTmin = 0;
		StringBuilder resultKey = new StringBuilder();
		
		//Initializing the data accumulating structure which will hold average temperature for a particular station across the years
		Map<Integer,StationInstanceInCombine> stationByYearDetails = new LinkedHashMap<>();


		// Ensuring accumulation of temperature and count for a particular key i.e.Year data was recorded
		for (StationInstanceInCombine station : values){

			accTmin = station.getAccTmin();
			accTmax = station.getAccTmax();
			countTmax = station.getCountTmax();
			countTmin = station.getCountTmin();

			if(stationByYearDetails.containsKey(key.getYear())) {

				stationByYearDetails.get(key.getYear()).updateStationInstance(accTmax, accTmin, countTmax, countTmin);
			}else{

				stationByYearDetails.put(key.getYear(),new StationInstanceInCombine(accTmax, accTmin, countTmax, countTmin));
			}			
		}


		//Creating output in the format excepted
		resultKey.append(key.getStationID()+", [");
		int count = stationByYearDetails.size();
		for(Integer i : stationByYearDetails.keySet()){
			if( count == 1){
				resultKey.append("(")
				.append("" + i)
				.append(", ")
				.append((stationByYearDetails.get(i).getCountTmin() == 0)?"Does not exist":"" + stationByYearDetails.get(i).getAccTmin()/stationByYearDetails.get(i).getCountTmin())
				.append(", ")
				.append((stationByYearDetails.get(i).getCountTmax() == 0)?"Does not exist":"" + stationByYearDetails.get(i).getAccTmax()/stationByYearDetails.get(i).getCountTmax())
				.append(")]");

			}else{				
					resultKey.append("(")
					.append("" + i)
					.append(", ")
					.append((stationByYearDetails.get(i).getCountTmin() == 0)?"Does not exist":"" + stationByYearDetails.get(i).getAccTmin()/stationByYearDetails.get(i).getCountTmin())
					.append(", ")
					.append((stationByYearDetails.get(i).getCountTmax() == 0)?"Does not exist":"" + stationByYearDetails.get(i).getAccTmax()/stationByYearDetails.get(i).getCountTmax())
					.append("),");				
			}
			count--;
		}
		
		// Emit the key-value pair
		context.write(new Text(resultKey.toString()), result);
	}
}



// Mapper Class
class CsvFileReaderMapper extends Mapper<Object, Text, CompositeKey, StationInstanceInCombine> {

	//Creating a accumulated structure to perform efficient combining
	Map<CompositeKey,StationInstanceInCombine> stationDetails;

	//Initializing the data accumulating structure
	public void setup(Context context) {
		stationDetails = new HashMap<CompositeKey,StationInstanceInCombine>();
	}

	// Map function which emits StationId as key and customized StationInstanceInCombine class as value
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Split the input based on the records in each line
		String[] lines = value.toString().split("\n");

		for (String line : lines) {

			// Consider only those records which contains the following pattern
			if (line.contains("TMAX") || line.contains("TMIN")) {


				String[] recordComponents = line.split(",");

				String stationID = recordComponents[0];

				Integer year = Integer.parseInt(recordComponents[1].substring(0,4));

				CompositeKey compositeMapKey = new CompositeKey(stationID, year); 

				if(recordComponents[2].equals("TMAX")) {

					if (stationDetails.containsKey(compositeMapKey)){

						// Extracting the maximum temperature value from the record and updating the existing station details
						// in the accumulated data structure
						stationDetails.get(compositeMapKey).updateTmax(Double.parseDouble(recordComponents[3]));

					}else{

						// Extracting the maximum temperature value from the record and inserting station details
						// in the accumulated data structure
						stationDetails.put(compositeMapKey, new StationInstanceInCombine(Double.parseDouble(recordComponents[3]),0.0,1,0));
					}
				}

				if(recordComponents[2].equals("TMIN")){

					if (stationDetails.containsKey(compositeMapKey)){

						// Extracting the minimum temperature value from the record and updating the existing station details
						// in the accumulated data structure
						stationDetails.get(compositeMapKey).updateTmin(Double.parseDouble(recordComponents[3]));

					}else{

						// Extracting the minimum temperature value from the record and inserting station details
						// in the accumulated data structure
						stationDetails.put(compositeMapKey, new StationInstanceInCombine(0.0,Double.parseDouble(recordComponents[3]),0,1));
					}


				}
			}
		}
	}

	// Once the accumulating structure is completely populated by the input, it is emitted
	public void cleanup(Context context) throws IOException, InterruptedException { 
		for(CompositeKey s : stationDetails.keySet()){

			context.write(s,stationDetails.get(s));
		}
	}
}




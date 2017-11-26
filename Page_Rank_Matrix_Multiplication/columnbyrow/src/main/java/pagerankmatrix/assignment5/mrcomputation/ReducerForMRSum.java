package pagerankmatrix.assignment5.mrcomputation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForMRSum extends Reducer<LongWritable, DoubleWritable, Text, NullWritable>{
	
	Long enhancedDanglingFactor;
	double danglingFactor;
	Long noOfNodes;
	private static final double RANDOM_JUMP_PROBABILITY = 0.15;
	private static final long DAMPLING_DIVISION_FACTOR = 10000000000000000l;
	private NullWritable nw = NullWritable.get();
	
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		enhancedDanglingFactor = conf.getLong("DanglingFactor",1);
		danglingFactor = (Double.valueOf(enhancedDanglingFactor)/DAMPLING_DIVISION_FACTOR);
		noOfNodes = conf.getLong("TOTALNODES",1);
		
		
	}
	
	// Computing the page rank for value for each pages based on inline contribution received from mapper
	public void reduce(LongWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		
		double pageRankScore = 0.0;
		double accumulatedContribution = 0.0;
		
		for(DoubleWritable val : values){
			
			accumulatedContribution = accumulatedContribution + val.get();
			
		}
		
		pageRankScore = RANDOM_JUMP_PROBABILITY/noOfNodes + (1.0 - RANDOM_JUMP_PROBABILITY)*(danglingFactor + accumulatedContribution);
		
		context.write(new Text(key.toString()+"##"+pageRankScore), nw);
	}

}

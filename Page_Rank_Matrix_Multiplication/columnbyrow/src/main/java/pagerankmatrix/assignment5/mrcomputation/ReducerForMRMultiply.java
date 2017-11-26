package pagerankmatrix.assignment5.mrcomputation;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerForMRMultiply extends Reducer<KeyForMRComputation, ValueForMRComputation, Text, NullWritable>{

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

	// Computing inlink contribution for all nodes
	public void reduce(KeyForMRComputation key, Iterable<ValueForMRComputation> values, Context context) throws IOException, InterruptedException {

		double parentPageRankScore = 0.0;
		int counter = 0;
		
		for(ValueForMRComputation val : values){

			if(counter == 0){
				if(key.getMatrixName().equals("R")){

					// Extracting the value of page rank
					parentPageRankScore = val.getPageRankContributionValue();
					
				}else{
					
					// handling dead node by sending dummy value which will not affect the page rank score of score
					parentPageRankScore = RANDOM_JUMP_PROBABILITY/noOfNodes + (1.0 - RANDOM_JUMP_PROBABILITY)*danglingFactor;
					context.write(new Text(key.getPageIndex()+"##"+0.0), nw);
					context.write(new Text(val.getOutlinksPageIndex()+"##"+(parentPageRankScore*val.getPageRankContributionValue())), nw);

				}
				
				counter++;
				
			}else{
				
				double inlinkContribution = 0.0;
				
				// Extracting the contribution to a particular node by parent node
				inlinkContribution = parentPageRankScore*val.getPageRankContributionValue();
				context.write(new Text(val.getOutlinksPageIndex()+"##"+inlinkContribution), nw);

			}
		}

	}
	
}

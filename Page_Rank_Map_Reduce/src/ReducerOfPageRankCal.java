package pagerank.assignment3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Outputs in a format which can be read by the mapper in the next iteration
public class ReducerOfPageRankCal extends Reducer<Text,NodeDetails,Text,NullWritable>{

	private NullWritable result = NullWritable.get();
	private double danglingFactor;
	private long noOfNodes;
	private long updatedNoOfNodes;
	private double pageRankValue;
	private String nodesText;
	private static final double RANDOM_JUMP_PROBABILITY = 0.15;
	private static final long DAMPLING_DIVISION_FACTOR = 100000000000000l;
	private double accumulatedOtherPageRank;
	private StringBuilder outputFormat;


	// Setting up all parameters required for page rank calculation from job configuration
	public void setup(Context context) {

		Configuration conf = context.getConfiguration();
		// retrieving Dangling factor from configuration
		long enhancedDanglingFactor = context.getConfiguration().getLong("DanglingFactor",1);
		danglingFactor = (Double.valueOf(enhancedDanglingFactor)/DAMPLING_DIVISION_FACTOR);
		noOfNodes = context.getConfiguration().getLong("TOTALNODES", 1);
	}

	// Updating the page rank values of nodes and emitting in the format required by the mapper in the next iteration
	public void reduce(Text key, Iterable<NodeDetails> values, Context context) throws IOException, InterruptedException {

		outputFormat = new StringBuilder();
		nodesText = "";
		accumulatedOtherPageRank = 0.0;

		for(NodeDetails node : values){

			if(node.isHasOutlinks()){

				nodesText = node.getOutlinks();

			}else{

				accumulatedOtherPageRank = accumulatedOtherPageRank + (double)(node.getOtherPageRankValue()/node.getOtherInlink());
			}	

		}

		// Updating page rank value of the node
		pageRankValue = (double)(RANDOM_JUMP_PROBABILITY/noOfNodes) + (double)(1.0 - RANDOM_JUMP_PROBABILITY)*((double)(danglingFactor/noOfNodes) + accumulatedOtherPageRank);

		outputFormat.append(key.toString());
		outputFormat.append("##");
		outputFormat.append(nodesText);
		outputFormat.append("##");
		outputFormat.append(pageRankValue);

		context.write(new Text(outputFormat.toString()), result);

		// Updating the total number of unique nodes found in this iteration
		updatedNoOfNodes++;


	}


	// Updating the global counter in order to make use of it in the next iteration
	public void cleanup(Context context) throws IOException, InterruptedException { 

		context.getCounter(PAGE_RANK_COUNTER.NO_OF_NODES).setValue(updatedNoOfNodes);

	}

}

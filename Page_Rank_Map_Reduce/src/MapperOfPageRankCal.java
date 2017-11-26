package pagerank.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.conf.Configuration;

public class MapperOfPageRankCal extends Mapper<Object, Text, Text, NodeDetails> {

	private int iteration;
	private long danglingFactor;
	private long noOfNodes;
	private double pageRankValue;
	private static final long DAMPLING_MULTIPLE_FACTOR = 100000000000000l;


	// Setting up all the required parameters before the map execution for calculation of Dangling factor
	public void setup(Context context) {

		// Getting the parameters set in configuration for this iteration
		Configuration conf = context.getConfiguration();
		this.danglingFactor = conf.getLong("DanglingFactor",1);
		noOfNodes = conf.getLong("TOTALNODES",1);
		iteration = conf.getInt("itr", 1);

		// For first iteration, setting the initial page rank value to all nodes
		if(iteration == 0){
			pageRankValue = 1.0/noOfNodes;
		}

	}


	
	// Along with updation of Dangling factor for this iteration, emitting essential details to update 
	// page rank value of each node in reducer.
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		
		String line = value.toString();
		
		// In order to ensure that .success file is not read
		if(line.contains("##")){
		
		// Extracting node name, list of outlinks and its corresponding page rank
		String[] nodeSplit = value.toString().split("##");
		
		String nodeID = nodeSplit[0];
		pageRankValue = (iteration == 0)? pageRankValue : Double.parseDouble(nodeSplit[2]);
		String nodeSplit1Val = nodeSplit[1].trim();
		
		String[] nodeOutlinkSplit = null;
		
		if(!nodeSplit1Val.isEmpty()){
			nodeOutlinkSplit = nodeSplit1Val.split("~");
		}
				
		// If no outinks for node just update the Dampling factor
		if(null == nodeOutlinkSplit || nodeOutlinkSplit.length == 0){
			
			context.getCounter(PAGE_RANK_COUNTER.DANGLING_FACTOR).increment((long)(pageRankValue*DAMPLING_MULTIPLE_FACTOR));
			
		}else{
			
			String[] nodeList = nodeSplit[1].trim().split("~");
			
			// iterating the list of outlinks
			for(String node : nodeList){
				
				if(node.length() != 0){
					
					// emit parameters to update page rank of outlinks in the reducer
					context.write(new Text(node), new NodeDetails(new Long(nodeOutlinkSplit.length),new Double(pageRankValue)));
					
				}
			}
		}
		
		// emitting the node itself along with its outlinks irrespective it is present or not
		context.write(new Text(nodeID), new NodeDetails(nodeSplit[1].trim(),true));
		
	}

	}

	
	public void cleanup(Context context) throws IOException, InterruptedException { 

		// No need for clean up as all actions are performed in map call
	}

}

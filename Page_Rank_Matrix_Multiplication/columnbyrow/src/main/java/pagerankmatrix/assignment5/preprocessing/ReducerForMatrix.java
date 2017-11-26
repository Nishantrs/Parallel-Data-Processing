package pagerankmatrix.assignment5.preprocessing;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
//import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import pagerankmatrix.assignment5.PAGE_RANK_COUNTER;

// Reducer generates 4 matrices: D-Matrix,M-Matrix,R-Matrix,IndexMapping-Matrix
public class ReducerForMatrix extends Reducer<Text, Text, Text, NullWritable> {

	// Store a map of page name and its index
	Map<String, Long> pageToNodeRepresentation;
	Set<Long> danglingNodes;
	Long nodeRepresentation;
	private MultipleOutputs<Text, NullWritable> mos; //removed raw type
	private NullWritable nw = NullWritable.get();

	public void setup(Context context) {

		pageToNodeRepresentation = new HashMap<>(); //removed linked HashMap
		nodeRepresentation = 0l;
		danglingNodes = new HashSet<>();
		mos = new MultipleOutputs(context);

	}

	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String pageName = new String();
		Long pageRepresentation = 0l;
		Long outlinkRepresentation = 0l;
		StringBuilder output = new StringBuilder();
		Boolean isDangling = true;
		int flag = 0;
		NullWritable nw = NullWritable.get();
		

		pageName = key.toString().trim();

		// If page already assigned an index, get index else assign one to it
		// And write it to index mapper matrix
		if (!pageName.isEmpty()) {
			
			if (pageToNodeRepresentation.containsKey(pageName)) {

				pageRepresentation = pageToNodeRepresentation.get(pageName);

			} else {

				pageToNodeRepresentation.put(pageName, nodeRepresentation);
				pageRepresentation = nodeRepresentation;
				mos.write("IndexMapping",new Text(pageName + "##" + pageRepresentation), nw);
				nodeRepresentation++;

			}
		}


		for (Text outlink : values) {

			String outlinkString = outlink.toString().trim();

			if (!outlinkString.isEmpty()) {

				// If page has at least one outlink it is not dangling
				isDangling = false;
				
				// If it is the first iteration, create following output
				if (flag == 0) {
					
					output.append(pageRepresentation);
					output.append("##");
				} 

				
				// If outlink already assigned an index, get index else assign one to it
				// And write it to index mapper matrix 
				if (pageToNodeRepresentation.containsKey(outlinkString)) {

					outlinkRepresentation = pageToNodeRepresentation.get(outlinkString);

				} else {

					pageToNodeRepresentation.put(outlinkString,nodeRepresentation);
					outlinkRepresentation = nodeRepresentation;
					mos.write("IndexMapping",new Text(outlinkString+ "##" + outlinkRepresentation), nw);
					nodeRepresentation++;
				}

				
				// Logic to compute output in page##outlink1~outlink2~outlink3 format
				if (flag == 0) {

					output.append(outlinkRepresentation);
					
				} else {

					output.append("~" + outlinkRepresentation);
				}

				flag++;



			}
		}

		// M Matrix is updated in here
		// If dangling node add to list else update M-Matrix
		if(isDangling){

			danglingNodes.add(pageRepresentation);
			
		}else{

			mos.write("MMatrix", new Text(output.toString()), nw);
		}

	}


	public void cleanup(Context context) throws IOException,InterruptedException {
		
		Long countPages = (long) pageToNodeRepresentation.size();
		Double initialPageRank = (1.0 / countPages);
		
		// Updating D-matrix and R-matrix
		for (Map.Entry<String, Long> entry : pageToNodeRepresentation
				.entrySet()) {
			mos.write("RMatrix", new Text(entry.getValue() + "##"
					+ initialPageRank), nw);

			if(danglingNodes.contains(entry.getValue())){

				mos.write("DMatrix",
						new Text(entry.getValue() + "##" + initialPageRank), nw);
			}
		}


		mos.close();

		context.getCounter(PAGE_RANK_COUNTER.NO_OF_NODES).setValue(countPages);

	}

}

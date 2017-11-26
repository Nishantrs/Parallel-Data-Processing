package pagerankmatrix.assignment5.topknodes;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.google.common.collect.Iterables;

public class ReducerForTopKNodes extends Reducer<LongWritable,PageNameOrPageRank,Text,DoubleWritable>{
	

	// Code partially referred from Stack Overflow
	private Map<String, Double> unsorted;
	private Map<String, Double> reverseSortedByValues;

	// Method to return a Map based on value in the descending order
	private static Map<String, Double> sortByComparator(Map<String, Double> unsortMap)
	{

		List<Entry<String, Double>> list = new LinkedList<Entry<String, Double>>(unsortMap.entrySet());

		// Sorting the list based on values
		Collections.sort(list, new Comparator<Entry<String, Double>>()
				{
			public int compare(Entry<String, Double> o1, Entry<String, Double> o2)
			{
				return o2.getValue().compareTo(o1.getValue());                
			}
				});

		// Maintaining insertion order with the help of LinkedList
		Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
		for (Entry<String, Double> entry : list)
		{
			sortedMap.put(entry.getKey(), entry.getValue());
		}

		return sortedMap;
	}
			
		// Counter to ensure only 100 nodes emitted
		private int counter;

		// Initializing an accumulated data structure for nodes and corresponding page ranks
		public void setup(Context context) {

			unsorted = new HashMap<String, Double>();
			counter = 100;

		}
		
		// Ensuring all node and their corresponding page rank are stored in the accumulated data structure
		// valid records will come in pairs of page rank and page name
		// Else there will be just page name
		public void reduce(LongWritable key, Iterable<PageNameOrPageRank> values, Context context) throws IOException, InterruptedException {

			
			int count = 0;
			
			double pageRankValue = 0.0;
			String pageName = new String();
			

			for(PageNameOrPageRank node : values){
				
				if(node.getIsName()){
					
					pageName = node.getPageName();

				}else{
					
					pageRankValue = node.getPageRank();

				}
				
				count++;
				
			}
			
			if(count == 2)
			unsorted.put(pageName, pageRankValue);

		}

		// Emitting the top 100 entries in the map the reducer.
		public void cleanup(Context context) throws IOException, InterruptedException { 

			reverseSortedByValues = sortByComparator(unsorted);

			for (Entry<String, Double> entry : reverseSortedByValues.entrySet())
			{
				context.write(new Text(entry.getKey()),new DoubleWritable(entry.getValue()));
				counter --;

				if(counter <= 0){
					break;
				}	
			}

		}
}

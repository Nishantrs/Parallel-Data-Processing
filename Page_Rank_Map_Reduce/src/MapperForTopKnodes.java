package pagerank.assignment3;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

// Using in mapper combining to emit top 100 nodes
public class MapperForTopKnodes extends Mapper<Object, Text,DoubleWritable, Text>{


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

	// Initializing an accumulated data structure for nodes and corresponding page ranks
	public void setup(Context context) {

		unsorted = new HashMap<String, Double>();

	}

	// Ensuring all node and their corresponding page rank are stored in the accumulated data structure
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


		String line = value.toString();

		// In order to ensure that .success file is not read
		if(line.contains("##")){
			
			String[] nodeSplit = value.toString().split("##");

			String nodeID = nodeSplit[0];

			Double pakeRankValue = Double.parseDouble(nodeSplit[2]);

			unsorted.put(nodeID, pakeRankValue);
		}

	}

	// Emitting the top 100 entries in the map the reducer.
	public void cleanup(Context context) throws IOException, InterruptedException { 

		int counter = 100;


		reverseSortedByValues = sortByComparator(unsorted);

		for (Entry<String, Double> entry : reverseSortedByValues.entrySet())
		{

			//System.out.println("Key : " + entry.getKey() + " Value : "+ entry.getValue());
			context.write(new DoubleWritable(entry.getValue()),new Text(entry.getKey()));
			counter --;

			if(counter <= 0){
				break;
			}			

		}

	}

}





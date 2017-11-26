package pagerankmatrix.assignment5rbc.topknodes;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//Reading from the final page rank matrix
public class MapperForReadingRMatrixFinal extends Mapper<Object, Text, LongWritable, PageNameOrPageRank>{
	
	// Code partially referred from Stack Overflow
		private Map<Long, Double> unsorted;
		private Map<Long, Double> reverseSortedByValues;
		
		// Method to return a Map based on value in the descending order
		private static Map<Long, Double> sortByComparator(Map<Long, Double> unsortMap)
		{

			List<Entry<Long, Double>> list = new LinkedList<Entry<Long, Double>>(unsortMap.entrySet());

			// Sorting the list based on values
			Collections.sort(list, new Comparator<Entry<Long, Double>>()
					{
				public int compare(Entry<Long, Double> o1, Entry<Long, Double> o2)
				{
					return o2.getValue().compareTo(o1.getValue());                
				}
					});

			// Maintaining insertion order with the help of LinkedList
			Map<Long, Double> sortedMap = new LinkedHashMap<Long, Double>();
			for (Entry<Long, Double> entry : list)
			{
				sortedMap.put(entry.getKey(), entry.getValue());
			}

			return sortedMap;
		}
		
		
		// Initializing an accumulated data structure for nodes and corresponding page ranks
		public void setup(Context context) {

			unsorted = new HashMap<Long, Double>();

		}
		
		// Ensuring all node and their corresponding page rank are stored in the accumulated data structure
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {


			String line = value.toString().trim();

			// In order to ensure that .success file is not read
			if(line.contains("##")){
				
				String[] nodeSplit = line.split("##");

				Long pageIndex = Long.parseLong(nodeSplit[0].trim());

				Double pageRankValue = Double.parseDouble(nodeSplit[1].trim());

				unsorted.put(pageIndex, pageRankValue);
			}

		}
		
		
		// Emitting the top 100 entries based on page rank score to the reducer.
		public void cleanup(Context context) throws IOException, InterruptedException { 

			int counter = 100;


			reverseSortedByValues = sortByComparator(unsorted);
			

			for (Entry<Long, Double> entry : reverseSortedByValues.entrySet())
			{
				context.write(new LongWritable(entry.getKey()),new PageNameOrPageRank("",entry.getValue(),false));
				counter --;

				
				if(counter <= 0){
					break;
				}			

			}

		}



}

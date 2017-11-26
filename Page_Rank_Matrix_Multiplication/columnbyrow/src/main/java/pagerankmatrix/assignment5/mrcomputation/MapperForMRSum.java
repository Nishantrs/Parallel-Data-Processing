package pagerankmatrix.assignment5.mrcomputation;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForMRSum extends Mapper<Object, Text, LongWritable, DoubleWritable>{

	// Reading inlink contributions to a node and emitting it to calculate page rank in reduce phase
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();
		
		if(line.isEmpty() || 
				  !line.contains("##") || 
				  (line.split("##").length != 2)){
			
			return;
		}
		
		
		String[] multiplyMRRecord = line.split("##");
		Long pageIndex = Long.parseLong(multiplyMRRecord[0].trim());
		Double pageRankContribution = Double.parseDouble(multiplyMRRecord[1].trim());
		
		context.write(new LongWritable(pageIndex),new DoubleWritable(pageRankContribution));

	}
}

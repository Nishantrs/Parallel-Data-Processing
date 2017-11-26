package pagerankmatrix.assignment5.mrcomputation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForReadingRMatrixmr extends Mapper<Object, Text,KeyForMRComputation,ValueForMRComputation>{

	// Reading the page rank matrix R and emitting its content
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		
		String line = value.toString().trim();
		
		if(line.isEmpty() || 
				  !line.contains("##") || 
				  (line.split("##").length != 2)){

			return;
		}

		
		String[] rMatrixRecord = line.split("##");
		Long pageIndex = Long.parseLong(rMatrixRecord[0].trim());
		Double pageRankValue = Double.parseDouble(rMatrixRecord[1].trim());
		
		context.write(new KeyForMRComputation(pageIndex, "R"),new ValueForMRComputation(0l, pageRankValue));

	}
}

package pagerankmatrix.assignment5rbc.topknodes;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


//Reading the index-mapping file
public class MapperForReadingIndexMapping extends Mapper<Object, Text,LongWritable,PageNameOrPageRank>{
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		
		String line = value.toString().trim();
		
		if(line.isEmpty() || 
				  !line.contains("##") || 
				  (line.split("##").length != 2)){
			
			return;
		}
			
		String[] rMatrixRecord = line.split("##");
		Long pageIndex = Long.parseLong(rMatrixRecord[1].trim());
		String pageName = rMatrixRecord[0].trim();
		
		context.write(new LongWritable(pageIndex), new PageNameOrPageRank(pageName,0.0, true));
	}

}

package pagerankmatrix.assignment5.drcomputation;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import pagerankmatrix.assignment5.MatrixRecords;

public class MapperForReadingDMatrix extends Mapper<Object, Text, LongWritable, MatrixRecords>{

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String line = value.toString().trim();
		
		if(line.isEmpty() || 
		  !line.contains("##") || 
		  (line.split("##").length != 2)){
			
			return;
		}
		
		
		String[] dMatrixRecord = line.split("##");
		Long pageIndex = Long.parseLong(dMatrixRecord[0].trim());
		Double danglingContribution = Double.parseDouble(dMatrixRecord[1].trim());
		
		// Emit record containing information of matrix, dangling node and its dangling contribution
		context.write(new LongWritable(pageIndex),new MatrixRecords("D", pageIndex, danglingContribution));


	}
}

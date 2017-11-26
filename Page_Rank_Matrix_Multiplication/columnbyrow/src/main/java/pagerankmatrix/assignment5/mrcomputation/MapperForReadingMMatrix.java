package pagerankmatrix.assignment5.mrcomputation;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperForReadingMMatrix extends Mapper<Object, Text,KeyForMRComputation,ValueForMRComputation>{

	// Reading outlinks M-Matrix and emitting contribution factor of parent page to each outlink
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
		String line = value.toString().trim();
		
		if(line.isEmpty() || 
				  !line.contains("##") || 
				  (line.split("##").length != 2)){

			return;
		}

		
		String[] mMatrixRecord = line.split("##");
		Long pageIndex = Long.parseLong(mMatrixRecord[0].trim());
		String nodeSplitVal = mMatrixRecord[1].trim();

		String[] nodeOutlinkSplit = null;		
		if(!nodeSplitVal.isEmpty()){
			nodeOutlinkSplit = nodeSplitVal.split("~");
		}

		if(nodeOutlinkSplit == null || nodeOutlinkSplit.length == 0){
			return;
		}		

		int outlinksCount = nodeOutlinkSplit.length;
		double contributionFactor = 1.0/outlinksCount;
		
		for(String node : nodeOutlinkSplit){

			if(node.length() != 0){

				context.write(new KeyForMRComputation(pageIndex, "M"),
						new ValueForMRComputation(Long.parseLong(node), contributionFactor));
				
			}
		}


	}
}

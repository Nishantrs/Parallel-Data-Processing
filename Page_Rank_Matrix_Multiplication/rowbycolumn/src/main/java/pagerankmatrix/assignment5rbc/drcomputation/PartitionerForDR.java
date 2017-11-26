package pagerankmatrix.assignment5rbc.drcomputation;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import pagerankmatrix.assignment5rbc.MatrixRecords;

//Ensure all records go to one reducer
public class PartitionerForDR extends Partitioner<LongWritable, MatrixRecords>{


	@Override
	public int getPartition(LongWritable key, MatrixRecords value, int numReduceTasks) {

		return 0;

	}

}

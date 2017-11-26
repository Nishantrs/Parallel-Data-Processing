package pagerankmatrix.assignment5.mrcomputation;

import org.apache.hadoop.mapreduce.Partitioner;

// Ensuring all records are distributed across reducers based on key value
public class PartitionerForMR extends Partitioner<KeyForMRComputation, ValueForMRComputation>{

	@Override
	public int getPartition(KeyForMRComputation key,
			ValueForMRComputation value, int numReduceTasks) {
		// TODO Auto-generated method stub
		return (int)(key.getPageIndex().hashCode()%numReduceTasks);
	}

}

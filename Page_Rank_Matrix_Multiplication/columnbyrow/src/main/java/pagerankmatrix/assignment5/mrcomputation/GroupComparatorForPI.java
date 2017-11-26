package pagerankmatrix.assignment5.mrcomputation;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Records appearing at reducer must be grouped by key, which is page index
public class GroupComparatorForPI extends WritableComparator{

	protected GroupComparatorForPI() {

		super(KeyForMRComputation.class, true);
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		KeyForMRComputation key1 = (KeyForMRComputation) w1;
		KeyForMRComputation key2 = (KeyForMRComputation) w2;

		return key2.getPageIndex().compareTo(key1.getPageIndex());

	}

	
}

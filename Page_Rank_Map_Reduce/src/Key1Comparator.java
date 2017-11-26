package pagerank.assignment3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// Key comparator used in order to ensure that all records from every mapper appears in 
// sorted manner based on the page rank value at the reducer.
public class Key1Comparator extends WritableComparator {


	public Key1Comparator() {
		
		super(DoubleWritable.class, true);
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		DoubleWritable key1 = (DoubleWritable) w1;
		DoubleWritable key2 = (DoubleWritable) w2;

		int cmp = key2.compareTo(key1);

		if (cmp != 0) {

			return cmp;
		}

		return 0;
	}

}

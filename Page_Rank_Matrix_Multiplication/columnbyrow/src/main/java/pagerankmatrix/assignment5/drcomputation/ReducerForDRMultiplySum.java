package pagerankmatrix.assignment5.drcomputation;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import pagerankmatrix.assignment5.MatrixRecords;
import pagerankmatrix.assignment5.PAGE_RANK_COUNTER;

public class ReducerForDRMultiplySum extends Reducer<LongWritable, MatrixRecords, Text, NullWritable>{

	public void reduce(LongWritable key, Iterable<MatrixRecords> values, Context context) throws IOException, InterruptedException {

		int count = 0;
		double accumulatedDangling = 1.0;

		// Only records in D-Matrix and R-Matrix contribute to dangling value hence count must be 2
		for(MatrixRecords rec : values){

			accumulatedDangling = accumulatedDangling*rec.getPageRankContribution();
			count++;

			
			// Update global counter with value contributed by each reducer
			if(count == 2){
				
				context.getCounter(PAGE_RANK_COUNTER.DANGLING_FACTOR).increment((long)(accumulatedDangling*10000000000000000L));
			}
		}

	}

}

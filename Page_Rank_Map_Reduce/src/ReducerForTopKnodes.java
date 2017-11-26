package pagerank.assignment3;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

// here we assume only one reducer is present and multiple mapper maybe present
public class ReducerForTopKnodes extends Reducer<DoubleWritable,Text,Text,DoubleWritable>{

	// Counter to ensure only 100 nodes emitted
	private int counter;

	// Initialize the counter to 0
	public void setup(Context context){
		counter = 0;
	}

	// Emitting just top 100 nodes based on page rank in the file
	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String nodeName = new String();

		if(counter <= 100){
			for(Text node : values){

				nodeName = node.toString();

				if(counter > 100){
					break;
				}
				context.write(new Text(nodeName), key);
				counter++;

			}
		}
	}

}



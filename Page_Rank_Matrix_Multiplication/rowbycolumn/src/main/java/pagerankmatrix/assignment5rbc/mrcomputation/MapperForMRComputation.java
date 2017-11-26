package pagerankmatrix.assignment5rbc.mrcomputation;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

// Reading R-Matrix in set up and M-Matrix to compute inlink contributions from different pages
public class MapperForMRComputation  extends Mapper<Object, Text, LongWritable, DoubleWritable>{

	Map<Long,Double> rmatrix;
	private int iteration;
	String filePath;


	public void setup(Context context) {


		Configuration conf = context.getConfiguration();

		rmatrix = new HashMap<>();
		filePath = conf.get("Path");
		Path path = new Path(conf.get("Path"));
		iteration = conf.getInt("itr", 1);
		FileSystem fs;

		if(iteration != 0){
			try {

				// Code referenced from external source. Setting up HDFS for all iteration but first
				fs = FileSystem.get(URI.create(filePath),context.getConfiguration());

				FileStatus[] status = fs.listStatus(path);
				for (int i=0;i<status.length;i++){
					BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
					String line ="";
					while ((line = br.readLine())!= null){

						String[] rRecord = line.split("##");

						if(rRecord.length == 2){

							rmatrix.put(Long.parseLong(rRecord[0]),Double.parseDouble(rRecord[1]));
						}

					}
				}
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}else{

			// Code referenced from external source. Setting up distributed file cache for first iteration
			try {
				Path[] r_Matrix = DistributedCache.getLocalCacheFiles(context.getConfiguration());

				if(r_Matrix != null && r_Matrix.length > 0) {

					for(Path location : r_Matrix) {

						readFile(location);

					}

				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}


	}

	// Calculating inlink contribution for all the pages present
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if(value.toString().trim().isEmpty() || 
				!value.toString().trim().contains("##") || 
				(value.toString().trim().split("##").length > 2) ||
				(value.toString().trim().split("##").length < 2)){

			return;
		}

		String line = value.toString().trim();
		String[] mMatrixRecord = line.split("##");
		Long pageIndex = Long.parseLong(mMatrixRecord[0]);
		String nodeSplitVal = mMatrixRecord[1].trim();

		String[] nodeOutlinkSplit = null;		
		if(!nodeSplitVal.isEmpty()){
			nodeOutlinkSplit = nodeSplitVal.split("~");
		}

		if(null == nodeOutlinkSplit || nodeOutlinkSplit.length == 0){
			return;
		}		

		// Emit dummy for parent nodes to handle dead nodes
		context.write(new LongWritable(pageIndex),new DoubleWritable(0.0));
		for(String node : nodeOutlinkSplit){

			// Emit inlink contribution by parent nodes to all its outlinks
			context.write(new LongWritable(Long.parseLong(node)),new DoubleWritable(rmatrix.get(pageIndex)*1.0/nodeOutlinkSplit.length));

		}
	}

	// Code referenced from external source
	private void readFile(Path filePath) {

		try{

			BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));

			String row = "";

			while((row = bufferedReader.readLine())!= null) {

				String[] rRecord = row.split("##");

				if(rRecord.length == 2){

					rmatrix.put(Long.parseLong(rRecord[0]),Double.parseDouble(rRecord[1]));
				}			


			}

			bufferedReader.close();

		} catch(IOException ex) {

			System.err.println("Exception while reading stop words file: " + ex.getMessage());

		}

	}

}



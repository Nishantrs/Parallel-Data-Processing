package pagerank.assignment3;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {

	// Used to send total number of nodes from read job to page rank calculation job
	static long noOfNodes;
	// Initially set up to zero which gets updated for later iterations of page rank calculation job
	static long danglingFactor = 0l;
	
	// Used to store directory for last input file for printing top 100 nodes.
	private static String lastInputPath;
	
	//Maximum iteration value
	private static final int MAX = 9;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		Path input;
		if (otherArgs.length > 0) {
			input = new Path(otherArgs[0]);
		}
		else {
			input = new Path("data.tsv.bz2");
		}


		// First job called to parse the data from the given .bz2 file format using the given the input file parser code.
		readBz2File(conf, input);



		// Once the parsing is done, the second job is iterated 10 times in order to converge the PageRank value
		for(int ii = 0; ii < 10; ii++){
			
			StringBuilder newPath = new StringBuilder();
			newPath.append(input);

			System.out.println("Iteration No:"+ ii);			

			//iterateToCalculatePangeRank(conf, ii, tempPath.toString());
			iterateToCalculatePageRank(conf, ii, newPath.toString());

			if(ii == 9){

				lastInputPath = newPath.toString();
				System.out.println(lastInputPath);

			}
		}
		
		// Final job emitting the Top 100 websites based on Pank rank values.
		printTopKNodes(conf,lastInputPath, MAX,otherArgs[1]);
	}


	
	// Setting up the job for reading the input files
	public static void readBz2File(Configuration conf, Path input) throws Exception {

		Job job = Job.getInstance(conf, "read input");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(MapperForReading.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, input);
		
		StringBuilder newPath = new StringBuilder();
		String outputFolder = "/OutputOfFileParsingMapper";
		newPath.append(input);
		newPath.append(outputFolder);

		FileOutputFormat.setOutputPath(job, new Path(newPath.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// Updating total number of nodes to calculate page rank for first iteration of next job.
		noOfNodes = job.getCounters().findCounter(PAGE_RANK_COUNTER.NO_OF_NODES).getValue();
	}


	
	// Setting up the job for calculation of page rank score
	public static void iterateToCalculatePageRank(Configuration conf, int ii, String input) throws Exception {

		// Setting up configuration parameters before the execution of jobs
		conf.setInt("itr", ii);
		conf.setLong("TOTALNODES", noOfNodes);
		conf.setLong("DanglingFactor", danglingFactor);

		Job job = Job.getInstance(conf, "Page Rank iteration");

		job.setJarByClass(PageRank.class);
		job.setMapperClass(MapperOfPageRankCal.class);
		job.setReducerClass(ReducerOfPageRankCal.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NodeDetails.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);


		String inputFolder = (ii == 0) ? "/OutputOfFileParsingMapper" : "/OutputOfPageRank" + ii;
		
		StringBuilder newPathInput = new StringBuilder();
		newPathInput.append(input);
		newPathInput.append(inputFolder);
		
		FileInputFormat.addInputPath(job, new Path(newPathInput.toString()));
		
		StringBuilder newPathOutput = new StringBuilder();
		String outputFolder = "/OutputOfPageRank"+(ii+1);
		newPathOutput.append(input);
		newPathOutput.append(outputFolder);

		FileOutputFormat.setOutputPath(job, new Path(newPathOutput.toString()));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}

		// updating values of global class variables in order to ensure proper set up of configuration
		// for next iteration of job
		Counters counters  = job.getCounters();
		noOfNodes = counters.findCounter(PAGE_RANK_COUNTER.NO_OF_NODES).getValue();
		danglingFactor = counters.findCounter(PAGE_RANK_COUNTER.DANGLING_FACTOR).getValue();

	}
	
	
	
	// Setting up job for printing the Top K nodes based on its page rank value
	public static void printTopKNodes(Configuration conf, String input, int ii, String output) throws Exception {

		Job job = Job.getInstance(conf, "write output");
		job.setJarByClass(PageRank.class);
		job.setMapperClass(MapperForTopKnodes.class);
		job.setReducerClass(ReducerForTopKnodes.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(DoubleWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setSortComparatorClass(Key1Comparator.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		StringBuilder newPathInput = new StringBuilder();
		String inputFolder = "/OutputOfPageRank"+(ii+1);
		newPathInput.append(input);
		newPathInput.append(inputFolder);
		FileInputFormat.addInputPath(job, new Path(newPathInput.toString()));
		FileOutputFormat.setOutputPath(job, new Path(output));

		boolean ok = job.waitForCompletion(true);
		if (!ok) {
			throw new Exception("Job failed");
		}
	}

}

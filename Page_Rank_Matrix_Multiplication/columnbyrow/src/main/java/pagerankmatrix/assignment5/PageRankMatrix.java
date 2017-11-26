package pagerankmatrix.assignment5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import pagerankmatrix.assignment5.drcomputation.MapperForReadingDMatrix;
import pagerankmatrix.assignment5.drcomputation.MapperForReadingRMatrix;
import pagerankmatrix.assignment5.drcomputation.PartitionerForDR;
import pagerankmatrix.assignment5.drcomputation.ReducerForDRMultiplySum;
import pagerankmatrix.assignment5.mrcomputation.GroupComparatorForPI;
import pagerankmatrix.assignment5.mrcomputation.KeyForMRComputation;
import pagerankmatrix.assignment5.mrcomputation.MapperForMRSum;
import pagerankmatrix.assignment5.mrcomputation.MapperForReadingMMatrix;
import pagerankmatrix.assignment5.mrcomputation.MapperForReadingRMatrixmr;
import pagerankmatrix.assignment5.mrcomputation.PartitionerForMR;
import pagerankmatrix.assignment5.mrcomputation.ReducerForMRMultiply;
import pagerankmatrix.assignment5.mrcomputation.ReducerForMRSum;
import pagerankmatrix.assignment5.mrcomputation.ValueForMRComputation;
import pagerankmatrix.assignment5.preprocessing.MapperForReading;
import pagerankmatrix.assignment5.preprocessing.ReducerForMatrix;
import pagerankmatrix.assignment5.topknodes.MapperForReadingIndexMapping;
import pagerankmatrix.assignment5.topknodes.MapperForReadingRMatrixFinal;
import pagerankmatrix.assignment5.topknodes.PageNameOrPageRank;
import pagerankmatrix.assignment5.topknodes.ReducerForTopKNodes;


public class PageRankMatrix {
	
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
				input = new Path("/usr/local/nrs/MR/Assignment5");
			}
				

			// First job called to parse the data from the given .bz2 file format using the given the input file parser code.
			readBz2File(conf, input);

			// Once the parsing is done, the second job is iterated 10 times in order to converge the PageRank value
			for(int ii = 0; ii < 10; ii++){
				
				StringBuilder newPath = new StringBuilder();
				newPath.append(input);

				
				//Keeping track of iteration number
				System.out.println("Iteration No:"+ ii);			


				CalculateDRMultiplySum(conf, ii, newPath.toString());
				CalculateMRMultiply(conf, ii, newPath.toString());
				CalculateMRSum(conf, ii, newPath.toString());

				if(ii == 9){

					lastInputPath = newPath.toString();

				}
			}
			
			// Final job emitting the Top 100 websites based on Pank rank values.
			printTopKNodes(conf,lastInputPath, MAX,otherArgs[1]);
		}


		
		// Setting up the job for reading the input files
		public static void readBz2File(Configuration conf, Path input) throws Exception {

			Job job = Job.getInstance(conf, "read input");
			job.setJarByClass(PageRankMatrix.class);
			job.setMapperClass(MapperForReading.class);
			job.setReducerClass(ReducerForMatrix.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
			job.setNumReduceTasks(1);
			FileInputFormat.addInputPath(job, input);
			
			StringBuilder newPath = new StringBuilder();
			String outputFolder = "/OutputOfFileParsingMapper";
			newPath.append(input);
			newPath.append(outputFolder);
			//Multiple files: M-Matrix, R-Matrix, D-Matrix and IndexMapping-Matrix generated
			MultipleOutputs.addNamedOutput(job, "MMatrix", TextOutputFormat.class,
					 Text.class, NullWritable.class);
			MultipleOutputs.addNamedOutput(job, "RMatrix", TextOutputFormat.class,
					Text.class, NullWritable.class);
			MultipleOutputs.addNamedOutput(job, "DMatrix", TextOutputFormat.class,
					Text.class, NullWritable.class);
			MultipleOutputs.addNamedOutput(job, "IndexMapping", TextOutputFormat.class,
					Text.class, NullWritable.class);

			FileOutputFormat.setOutputPath(job, new Path(newPath.toString()));

			boolean ok = job.waitForCompletion(true);
			if (!ok) {
				throw new Exception("Job failed");
			}

			// Updating total number of nodes to calculate page rank for the rest of the iteration.
			noOfNodes = job.getCounters().findCounter(PAGE_RANK_COUNTER.NO_OF_NODES).getValue();
		}


		
		// Setting up the job for calculation DR (Dangling contribution) Matrix 
		public static void CalculateDRMultiplySum(Configuration conf, int ii, String input) throws Exception {

			// Setting up configuration parameters before the execution of jobs for dangling contribution for the 
			// next job: MR in the same iteration
			conf.setInt("itr", ii);
			conf.setLong("TOTALNODES", noOfNodes);
			conf.setLong("DanglingFactor", danglingFactor);

			Job job = Job.getInstance(conf, "DR Multiplication");
			
			// Creating paths for reading from and writing to different directory
			String pathOfDMatrix = "/OutputOfFileParsingMapper/DMatrix-r-00000";
			String pathOfRMatrix = (ii == 0) ? "/OutputOfFileParsingMapper/RMatrix-r-00000" : "/OutputOfPageRank/Iteration" + ii;
			StringBuilder newPathOfRMatrix = new StringBuilder();
			newPathOfRMatrix.append(input);
			newPathOfRMatrix.append(pathOfRMatrix);
			StringBuilder newPathOfDMatrix = new StringBuilder();
			newPathOfDMatrix.append(input);
			newPathOfDMatrix.append(pathOfDMatrix);
			
			StringBuilder newPathOutput = new StringBuilder();
			String outputFolder = "/OutputOfDM/Iteration"+(ii+1);
			newPathOutput.append(input);
			newPathOutput.append(outputFolder);
			
			

			job.setJarByClass(PageRankMatrix.class);
			// Reading file from two different location in two different map jobs: D-Matrix and R-Matrix
			MultipleInputs.addInputPath(job, new Path(newPathOfDMatrix.toString()), TextInputFormat.class, MapperForReadingDMatrix.class);
			MultipleInputs.addInputPath(job,new Path(newPathOfRMatrix.toString()), TextInputFormat.class, MapperForReadingRMatrix.class);
			job.setReducerClass(ReducerForDRMultiplySum.class);
			//job.setPartitionerClass(PartitionerForDR.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(MatrixRecords.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		

			FileOutputFormat.setOutputPath(job, new Path(newPathOutput.toString()));

			boolean ok = job.waitForCompletion(true);
			if (!ok) {
				throw new Exception("Job failed");
			}

			// updating values of global class variables in order to ensure proper set up of configuration
			// for next job in the same iteration
			Counters counters  = job.getCounters();
			danglingFactor = counters.findCounter(PAGE_RANK_COUNTER.DANGLING_FACTOR).getValue();


		}
		
		
		// Setting up the job for first step in calculation of MR (Outlinks contribution) Matrix
		public static void CalculateMRMultiply(Configuration conf, int ii, String input) throws Exception {

			// Setting up configuration parameters before the execution of jobs
			conf.setInt("itr", ii);
			conf.setLong("TOTALNODES", noOfNodes);
			conf.setLong("DanglingFactor", danglingFactor);
			

			Job job = Job.getInstance(conf, "MR Multiplication");
			
			
			// Creating paths for reading from and writing to different directory
			String pathOfMMatrix = "/OutputOfFileParsingMapper/MMatrix-r-00000";
			String pathOfRMatrix = (ii == 0) ? "/OutputOfFileParsingMapper/RMatrix-r-00000" : "/OutputOfPageRank/Iteration" + ii;
			StringBuilder newPathOfRMatrix = new StringBuilder();
			newPathOfRMatrix.append(input);
			newPathOfRMatrix.append(pathOfRMatrix);
			StringBuilder newPathOfMMatrix = new StringBuilder();
			newPathOfMMatrix.append(input);
			newPathOfMMatrix.append(pathOfMMatrix);
			StringBuilder newPathOutput = new StringBuilder();
			
			String outputFolder = "/OutputOfMRMultiply/Iteration"+(ii+1);
			newPathOutput.append(input);
			newPathOutput.append(outputFolder);

			job.setJarByClass(PageRankMatrix.class);
			// Reading file from two different location in two different map jobs: M-Matrix and R-Matrix
			MultipleInputs.addInputPath(job, new Path(newPathOfMMatrix.toString()), TextInputFormat.class, MapperForReadingMMatrix.class);
			MultipleInputs.addInputPath(job,new Path(newPathOfRMatrix.toString()), TextInputFormat.class, MapperForReadingRMatrixmr.class);
			job.setReducerClass(ReducerForMRMultiply.class);

			job.setMapOutputKeyClass(KeyForMRComputation.class);
			job.setMapOutputValueClass(ValueForMRComputation.class);
			job.setGroupingComparatorClass(GroupComparatorForPI.class);
			job.setPartitionerClass(PartitionerForMR.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);
		
			

			FileOutputFormat.setOutputPath(job, new Path(newPathOutput.toString()));

			boolean ok = job.waitForCompletion(true);
			if (!ok) {
				throw new Exception("Job failed");
			}
			
			

		}
		
		// Setting up the job for second step in calculation of MR (Page Rank value) Matrix
		public static void CalculateMRSum(Configuration conf, int ii, String input) throws Exception {

			// Setting up configuration parameters before the execution of jobs
			conf.setInt("itr", ii);
			conf.setLong("TOTALNODES", noOfNodes);
			conf.setLong("DanglingFactor", danglingFactor);

			Job job = Job.getInstance(conf, "MR Sum");

			job.setJarByClass(PageRankMatrix.class);

			job.setMapperClass(MapperForMRSum.class);
			job.setReducerClass(ReducerForMRSum.class);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(DoubleWritable.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(NullWritable.class);

			// Creating paths for reading from and writing to different directory
			String inputFolder = "/OutputOfMRMultiply/Iteration"+(ii+1);
			StringBuilder newPathInput = new StringBuilder();
			newPathInput.append(input);
			newPathInput.append(inputFolder);
			
			FileInputFormat.addInputPath(job, new Path(newPathInput.toString()));
			
			StringBuilder newPathOutput = new StringBuilder();
			String outputFolder = "/OutputOfPageRank/Iteration"+(ii+1);
			newPathOutput.append(input);
			newPathOutput.append(outputFolder);

			FileOutputFormat.setOutputPath(job, new Path(newPathOutput.toString()));

			boolean ok = job.waitForCompletion(true);
			if (!ok) {
				throw new Exception("Job failed");
			}
			
		}
		
		
		
		// Setting up job for printing the Top K nodes based on its page rank value
		public static void printTopKNodes(Configuration conf, String input, int ii, String output) throws Exception {

			Job job = Job.getInstance(conf, "write output");
			job.setJarByClass(PageRankMatrix.class);
			String pathOfIndexMappingMatrix = "/OutputOfFileParsingMapper/IndexMapping-r-00000";
			StringBuilder newPathOfIndexMappingMatrix = new StringBuilder();
			newPathOfIndexMappingMatrix.append(input);
			newPathOfIndexMappingMatrix.append(pathOfIndexMappingMatrix);
			
			
			String outputFolder = "/OutputOfPageRank/Iteration"+(ii+1);
			StringBuilder newPathOutput = new StringBuilder();
			newPathOutput.append(input);
			newPathOutput.append(outputFolder);
			// Reading file from two different location in two different map jobs: DIndexMapping-Matrix and R-Matrix
			MultipleInputs.addInputPath(job, new Path(newPathOfIndexMappingMatrix.toString()), TextInputFormat.class, MapperForReadingIndexMapping.class);
			MultipleInputs.addInputPath(job,new Path(newPathOutput.toString()), TextInputFormat.class, MapperForReadingRMatrixFinal.class);
			job.setReducerClass(ReducerForTopKNodes.class);
			job.setNumReduceTasks(1);
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(PageNameOrPageRank.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(DoubleWritable.class);
			

			FileOutputFormat.setOutputPath(job, new Path(output));

			boolean ok = job.waitForCompletion(true);
			if (!ok) {
				throw new Exception("Job failed");
			}
		}


}

package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;


public class GradientDescentDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new GradientDescentDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) {
		try {
			Configuration conf = new Configuration();
			int max_iters = 100;
			int num_iter = 1;
			double learning_rate = 0.001;

			Parameters.initializeParameters(2, learning_rate);

			while (num_iter < max_iters) {

				// Reset job configurations for each iteration
				Job job = Job.getInstance(conf, "GradientDescent");
				job.setJarByClass(GradientDescentDriver.class);

				// specify a Mapper
				job.setMapperClass(GradientDescentMapper.class);

				// specify a Reducer
				job.setReducerClass(GradientDescentReducer.class);

				// specify output types
				job.setOutputKeyClass(IntWritable.class);
				job.setOutputValueClass(DoubleWritable.class);

				job.setMapOutputKeyClass(IntWritable.class);
				job.setMapOutputValueClass(DoubleWritable.class);

				// specify input and output directories
				FileInputFormat.addInputPath(job, new Path(args[0]));
				job.setInputFormatClass(TextInputFormat.class);
				
				Path outPath = new Path(args[1] + '/' + num_iter);
				FileOutputFormat.setOutputPath(job, outPath);
				job.setOutputFormatClass(TextOutputFormat.class);
				job.waitForCompletion(false);
				//deleteOutputDirectory(args[1], job);
				System.out.println("Iteration: " + num_iter);
				num_iter++;
			}
			// // delete previous output
			// Path outputPathMid = new Path(args[1]);
			// FileSystem fs = FileSystem.get(outputPathMid.toUri(), job.getConfiguration());
			// fs.delete(outputPathMid, true);

			// FileOutputFormat.setOutputPath(job, outputPathMid);
			// job.setOutputFormatClass(TextOutputFormat.class);
			return 1;

		} catch (InterruptedException | ClassNotFoundException | IOException e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}

	private static void deleteOutputDirectory(String directory, Job job) throws IOException {
		Path dirPath = new Path(directory);
		FileSystem fs = FileSystem.get(dirPath.toUri(), job.getConfiguration());
		fs.delete(dirPath, true);
	}
}


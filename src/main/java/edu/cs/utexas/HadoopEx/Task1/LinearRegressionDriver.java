package edu.cs.utexas.HadoopEx.Task1;

import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class LinearRegressionDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		System.out.println(Arrays.toString(args));
		Configuration config = new Configuration();
		int res = ToolRunner.run(config, new LinearRegressionDriver(), args);

		if (res == 0) {
			double[] sums = new double[5];
			
			Path path = new Path(args[1] + "/part-r-00000");
			config.addResource(path);
			FileSystem fs = FileSystem.get(config);
			SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, config);
			IntWritable key = new IntWritable();
			DoubleWritable val = new DoubleWritable();
			while (reader.next(key, val)) {
				sums[key.get()] = val.get();
			}
			reader.close();

			double m;
			double b;

			m = (sums[4]*sums[2] - sums[0]*sums[1]) / (sums[4]*sums[3] - sums[0]*sums[0]);
			b = (sums[3]*sums[1] - sums[0]*sums[2]) / (sums[4]*sums[3] - sums[0]*sums[0]);

			System.out.println(Arrays.toString(sums));
			System.out.println("Slope: " + m);
			System.out.println("Intercept: " + b);

		}

		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String[] args) {
		try {
			Configuration conf = new Configuration();

			Job job = new Job(conf, "LinearRegression");
			job.setJarByClass(LinearRegressionDriver.class);

			// specify a Mapper
			job.setMapperClass(LinearRegressionMapper.class);

			// specify a Reducer
			job.setReducerClass(LinearRegressionReducer.class);
			job.setNumReduceTasks(1);

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setMapOutputValueClass(FloatWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			
			Path outputPath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);

			
			
			return (job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}

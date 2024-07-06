package edu.cs.utexas.HadoopEx.Task1;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.io.FilenameFilter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
			// List<String> lines = JobReader.getJobOutput(args[1]);
			// for(String line : lines) {
			// 	String[] split = line.split("\t");
			// 	sums[Integer.parseInt(split[0])] = Double.parseDouble(split[1]);
			// }
			File temp = new File(args[1]);
			File[] directory = temp.listFiles(new FilenameFilter() {
				public boolean accept(File dir, String name) {
					return !name.toLowerCase().endsWith(".crc");
				}
			});
			List<String> lines = new ArrayList<String>();
			if (directory != null) {
				for (File output : directory) {
					for (String line : Files.readString(output.toPath()).split("\\R")) {
						lines.add(line);
					}
				}
				for (String line : lines) {
					int index;
					try {
						String[] lineSplit = line.split("\\s+");
						index = Integer.parseInt(lineSplit[0]);
						sums[index] = Double.parseDouble(lineSplit[1]);
					} catch (NumberFormatException e) {
					}
				}
			}
			

			double m;
			double b;

			/*
			context.write(new IntWritable(0), new FloatWritable(x));
			context.write(new IntWritable(1), new FloatWritable(y));
			context.write(new IntWritable(2), new FloatWritable(x * y));
			context.write(new IntWritable(3), new FloatWritable(x * x));
			context.write(new IntWritable(4), new FloatWritable(1));
			*/

			m = (sums[4]*sums[2] - sums[0]*sums[1]) / (sums[4]*sums[3] - sums[0]*sums[0]);
			b = (sums[3]*sums[1] - sums[0]*sums[2]) / (sums[4]*sums[3] - sums[0]*sums[0]);

			System.out.println(Arrays.toString(sums));
			System.out.println("Slope: " + m);
			System.out.println("Intercept: " + b);

			// File results = new File(args[1] + "/task1Output");
			// results.createNewFile();
			// PrintWriter pw = new PrintWriter(results);

			// pw.write(String.format("Parameters:\n\tm: %f\n\tb: %f\n", m, b));

			//pw.close();
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

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);

			job.setMapOutputValueClass(FloatWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);
			
			Path outputPath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outputPath);
			job.setOutputFormatClass(TextOutputFormat.class);

			
			
			return (job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			System.err.println("Error during mapreduce job.");
			e.printStackTrace();
			return 2;
		}
	}
}

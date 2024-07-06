package edu.cs.utexas.HadoopEx.Task2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.List;

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

import edu.cs.utexas.HadoopEx.JobReader;

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
	public int run(String args[]) throws Exception {
		Configuration conf = new Configuration();
		conf.set("m", "0");
		conf.set("b", "0");

		File results = null;
		BufferedWriter bw = null;

		int max_iters = 100;
		int num_iter = 1;
		double learning_rate = 0.001;
		double prev_cost = -1.0;
		boolean terminate = false;

		while (!terminate && num_iter < max_iters) {
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

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			Path outPath = new Path(args[1] + '/' + num_iter);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setOutputFormatClass(TextOutputFormat.class);
			job.waitForCompletion(false);

			// read job output
			List<String> lines = JobReader.getJobOutput(outPath.toString());
			double cost = 0, m = 0, b = 0;
			for (String line : lines) {
				String[] split = line.split("\t");
				double parsed = Double.parseDouble(split[1]);
				switch(split[0]) {
					case "0":
						m = parsed;
						break;
					case "1":
						b = parsed;
						break;
					case "2":
						cost = parsed;
						break;
				}
			}

			if (prev_cost != -1.0 && cost > prev_cost) {
				learning_rate = learning_rate/5.0;
			} else if (cost < prev_cost) {
				learning_rate = learning_rate * 1.25;
			}

			// calculate new parameters on learning rate
			double new_m = Double.parseDouble(conf.get("m")) - learning_rate * m;
			double new_b = Double.parseDouble(conf.get("b")) - learning_rate * b;

			// update parameters
			conf.set("m", "" + new_m);
			conf.set("b", "" + new_b);

			// write to output file
			String writeOutput = String.format("Cost: %f\n\tm: %f\n\tb: %f\n", cost, m, b);

			if (bw == null) {
				results = new File(args[1] + "/finalOutputTask2");
				results.createNewFile();
				bw = new BufferedWriter(new FileWriter(results));
			}

			bw.write(writeOutput);
			bw.flush();

			// delete output directory (for cleanliness)
			FileSystem.get(outPath.toUri(), job.getConfiguration()).delete(outPath, true);

			System.out.println("Iteration: " + num_iter);
			System.out.println(writeOutput);
			num_iter++;
			if (prev_cost != -1.0 && Math.abs(cost - prev_cost)/prev_cost < 0.05) {
				terminate = true;
			}
			prev_cost = cost;
		}

		bw.close();
		return 1;
	}
}
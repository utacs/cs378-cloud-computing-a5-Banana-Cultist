package edu.cs.utexas.HadoopEx.Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MultiGradientDriver extends Configured implements Tool {

	/**
	 * 
	 * @param args
	 * @throws Exception
	 */

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new MultiGradientDriver(), args);
		System.exit(res);
	}

	/**
	 * 
	 */
	public int run(String args[]) throws Exception {
		Configuration conf = new Configuration();
		MultiGradientVector params = new MultiGradientVector(0.1);
		params.writeToConfiguration(conf);

		// File results = null;
		// BufferedWriter bw = null;

		int max_iters = 100;
		int num_iter = 1;
		double learning_rate = 0.001;
		double prev_cost = -1.0;
		boolean terminate = false;

		while (!terminate && num_iter < max_iters) {
			// Reset job configurations for each iteration
			Job job = Job.getInstance(conf, "MultiGradient");

			job.setJarByClass(MultiGradientDriver.class);

			// specify a Mapper
			job.setMapperClass(MultiGradientMapper.class);

			// specify a Reducer
			job.setReducerClass(MultiGradientReducer.class);

			// specify output types
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(DoubleWritable.class);

			// specify input and output directories
			FileInputFormat.addInputPath(job, new Path(args[0]));
			job.setInputFormatClass(TextInputFormat.class);

			Path outPath = new Path(args[1]);
			FileOutputFormat.setOutputPath(job, outPath);
			job.setOutputFormatClass(SequenceFileOutputFormat.class);
			job.waitForCompletion(false);

			// read job output
			// List<String> lines = JobReader.getJobOutput(outPath.toString());
			 double cost = -1.0;
			// for (String line : lines) {
			// 	String[] split = line.split("\t");
			// 	double parsed = Double.parseDouble(split[1]);
			// 	int index = Integer.parseInt(split[0]);
			// 	if (index >= MultiGradientVector.DEPENDENTS) {
			// 		cost = parsed;
			// 	} else {
			// 		params.vals[index] = parsed;
			// 	}
			// }

			for (int i = 0; i <= 2; ++i) {
				//System.out.println("i is: " + i);
				Path path = new Path(args[1] + "/part-r-0000" + i);
				//conf.addResource(path);
				FileSystem fs = FileSystem.get(conf);
				SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
				IntWritable key = new IntWritable();
				DoubleWritable val = new DoubleWritable();
				while (reader.next(key, val)) {
					//System.out.println("KEY: " + key.get() + ", VAL: " + val.get());
					if (key.get() >= MultiGradientVector.DEPENDENTS) {
						cost = val.get();
					} else {
						params.vals[key.get()] = val.get();
					}
				}
				reader.close();
			}

			if (prev_cost != -1.0 && cost > prev_cost) {
				learning_rate = learning_rate / 5.0;
			} else if (cost < prev_cost) {
				learning_rate = learning_rate * 1.25;
			}

			// calculate new parameters on learning rate
            params.updateParams(conf, learning_rate);

			// update parameters
			params.writeToConfiguration(conf);

			// write to output file
			String writeOutput = String.format("Cost: %f\nParams:\n%s\n", cost, params);
 
			// if (bw == null) {
			// 	results = new File(args[1] + "/finalOutputTask3");
			// 	results.createNewFile();
			// 	bw = new BufferedWriter(new FileWriter(results));
			// }

			// bw.write(writeOutput);
			// bw.flush();

			// delete output directory (for cleanliness)
			FileSystem.get(outPath.toUri(), job.getConfiguration()).delete(outPath, true);

			System.out.println("Iteration: " + num_iter);
			System.out.println(writeOutput);
			num_iter++;
			if (prev_cost != -1.0 && Math.abs(cost - prev_cost) < 100) {
				terminate = true;
			}
			prev_cost = cost;
		}

		//bw.close();
		return 0;
	}

	//check precision
	public static boolean checkTerminate(double[] new_vals, double[] old_vals) {
		for (int i = 0; i < new_vals.length;  ++i) {
			if (Math.abs(new_vals[i] - old_vals[i]) <= 0.000001) {
				return true;
			}
		}
		return false;
	}
}
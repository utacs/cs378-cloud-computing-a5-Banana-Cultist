package edu.cs.utexas.HadoopEx.Task1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LinearRegressionMapper extends Mapper<Object, Text, IntWritable, FloatWritable> {

	public static final int[] ROUNDING_TABLE = new int[] {2, 2, 4, 4, 0};

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {
		
		String[] columns = value.toString().split(",");
		if (clean(columns)) {
			float x = Float.parseFloat(columns[5]);
			float y = Float.parseFloat(columns[11]);
			context.write(new IntWritable(0), new FloatWritable(x));
			context.write(new IntWritable(1), new FloatWritable(y));
			context.write(new IntWritable(2), new FloatWritable(x * y));
			context.write(new IntWritable(3), new FloatWritable(x * x));
			context.write(new IntWritable(4), new FloatWritable(1)); // count
		}
	}

	public static boolean clean(String[] columns) {
		if (columns.length != 17) return false;

		try {
			int duration = Integer.parseInt(columns[4]);
			if (duration < 2 * 60 || duration > 60 * 60) {
				return false;
			}

			float fareAmount = Float.parseFloat(columns[11]);
			if (fareAmount < 3.00f || fareAmount > 200.00f) {
				return false;
			}

			float tripDistance = Float.parseFloat(columns[5]);
			if (tripDistance < 1.00f || tripDistance > 50.00f) {
				return false;
			}

			float tollAmount = Float.parseFloat(columns[15]);
			if (tollAmount < 3.00f) {
				return false;
			}
		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}
}

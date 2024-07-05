package edu.cs.utexas.HadoopEx.Task3;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiGradientMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {

	public void map(Object key, Text value, Context context) 
			throws IOException, InterruptedException {

		String[] columns = value.toString().split(",");
		if (this.clean(columns)) {
			//change from duration-in-seconds to duration-in-minutes
			double[] inputs = new double[MultiGradientVector.DEPENDENTS];
			

			inputs[0] = Double.parseDouble(columns[4]) / 60;
            inputs[1] = Double.parseDouble(columns[5]);
            inputs[2] = Double.parseDouble(columns[11]);
            inputs[3] = Double.parseDouble(columns[15]);

			inputs[4] = Double.parseDouble(columns[16]); // y

			Configuration conf = context.getConfiguration();

			MultiGradientVector params = new MultiGradientVector(conf);

			params.writePartialsCost(context, inputs);
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

			float totalAmount = Float.parseFloat(columns[16]);
		} catch (NumberFormatException e) {
			return false;
		}

		return true;
	}
}

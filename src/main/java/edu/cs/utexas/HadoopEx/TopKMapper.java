package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.PriorityQueue;


public class TopKMapper extends Mapper<Text, Text, Text, FloatWritable> {

	public static int K = 10;
	private PriorityQueue<TaxiRate> pq;

	public void setup(Context context) {
		pq = new PriorityQueue<>();
	}

	/**
	 * Reads in results from the first job and filters the topk results
	 *
	 * @param key
	 * @param errorRate a float value stored as a string
	 */
	public void map(Text key, Text errorRate, Context context)
			throws IOException, InterruptedException {

		pq.add(new TaxiRate(new Text(key), Float.parseFloat(errorRate.toString())));
		if (pq.size() > K) {
			pq.poll();
		}
	}

	public void cleanup(Context context) throws IOException, InterruptedException {
		while (!pq.isEmpty()) {
			TaxiRate taxi = pq.poll();
			context.write(taxi.id, new FloatWritable(taxi.rate));
		}
	}
}
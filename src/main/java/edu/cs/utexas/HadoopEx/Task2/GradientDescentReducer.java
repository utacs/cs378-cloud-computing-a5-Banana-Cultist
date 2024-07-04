package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;


public class GradientDescentReducer extends  Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
    public void reduce(IntWritable index, Iterable<DoubleWritable> gradients, Context context)
            throws IOException, InterruptedException {
        double sum = 0.0;
        int N = 0;
        for (DoubleWritable gradient : gradients) {
            sum += gradient.get();
            N++;
        }

        if (index.get() < 2) {
            sum = 2 * sum / N;
        }
        context.write(index, new DoubleWritable(sum));
    }
}

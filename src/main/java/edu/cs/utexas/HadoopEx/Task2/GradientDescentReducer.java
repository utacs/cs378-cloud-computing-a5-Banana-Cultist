package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;


public class GradientDescentReducer extends  Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {




    public void reduce(IntWritable index, Iterable<DoubleWritable> gradients, Context context) throws IOException, InterruptedException{
        double sum_gradients = 0.0;
        int N = 0;
        for (DoubleWritable gradient : gradients) {
            sum_gradients += gradient.get();
            N++;
        }

        sum_gradients = 2 * sum_gradients / N;
        try {
            Parameters.updateParameter(index.get(), sum_gradients);
        } catch (Exception e) {
            e.printStackTrace();
        }
        //context.write(index, new DoubleWritable(Parameters.parameters.get(index.get())));
    }

}

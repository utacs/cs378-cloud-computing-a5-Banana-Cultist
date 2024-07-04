package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;


public class GradientDescentReducer extends  Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {




    public void reduce(IntWritable index, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0.0;
        int N = 0;
        for (DoubleWritable value : values) {
            sum += value.get();
            N++;
        }

        if (index.get() != 2) {
            sum = 2 * sum / N;
            try {
                Parameters.updateParameter(index.get(), sum);
            } catch (Exception e) {
                e.printStackTrace();
            }
            context.write(index, new DoubleWritable(Parameters.parameters.get(index.get())));
        } else {
            context.write(index, new DoubleWritable(sum));
        }
    }

}

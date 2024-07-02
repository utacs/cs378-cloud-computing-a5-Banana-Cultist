package edu.cs.utexas.HadoopEx.Task1;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class LinearRegressionReducer extends  Reducer<IntWritable, FloatWritable, IntWritable, DoubleWritable> {
    public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        
        double sum = 0;
        int rounding = LinearRegressionMapper.ROUNDING_TABLE[key.get()];
        
        for (FloatWritable value : values) {
            sum += value.get();
            sum = round(sum, rounding);
        }
        
        context.write(key, new DoubleWritable(sum));
    }

    public static double round(double val, int place) {
        long factor = (long) Math.pow(10, place);
        return (double) Math.round(val * factor) / factor;
    }
}

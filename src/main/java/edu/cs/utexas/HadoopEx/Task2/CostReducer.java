package edu.cs.utexas.HadoopEx.Task2;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;
import java.util.ArrayList;


public class CostReducer extends  Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

    public void reduce(IntWritable index, Iterable<DoubleWritable> costs, Context context) throws IOException, InterruptedException{
        double sum_cost = 0.0;
        for (DoubleWritable cost : costs) {
            sum_cost += cost.get();
        }

        ArrayList<Double> parameters = Parameters.getParameters();
        double m = parameters.get(0);
        double b = parameters.get(1);
        System.out.println("Cost: " + sum_cost);
        System.out.println("Slope: " + m + ", y-intercept: " + b);
        context.write(index, new DoubleWritable(sum_cost));
    }

}

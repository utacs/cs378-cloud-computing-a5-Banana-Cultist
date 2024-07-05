package edu.cs.utexas.HadoopEx.Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;

import java.io.IOException;
import java.lang.StringBuilder;

public class MultiGradientVector {
    public final static int DEPENDENTS = 5;
    double[] vals = new double[5];

    public MultiGradientVector(double[] defaults) {
        vals = defaults;
    }

    public MultiGradientVector(double def) {
        for (int i = 0; i < DEPENDENTS; i++) {
            vals[i] = def;
        }
    }

    public MultiGradientVector(Configuration read) {
        for (int i = 0; i < DEPENDENTS; i++) {
            vals[i] = Double.parseDouble(read.get(i + ""));
        }
    }

    public void writeToConfiguration(Configuration conf) {
        for (int i = 0; i < DEPENDENTS; i++) {
            conf.set(i + "", vals[i] + "");
        }
    }

    public void updateParams(Configuration conf, double learningRate) {
        for (int i = 0; i < DEPENDENTS; i++) {
            vals[i] = Double.parseDouble(conf.get(i + "")) - learningRate * vals[i];
        }
    }

    public void writePartialsCost(Mapper<Object, Text, IntWritable, DoubleWritable>.Context context, 
        double[] inputs) throws IOException, InterruptedException {
        double diff = 0;
        for (int i = 0; i < DEPENDENTS - 1; i++) {
            diff += vals[i] * inputs[i];
        }
        diff += vals[DEPENDENTS - 1];
        diff = inputs[DEPENDENTS - 1] - diff;
        for (int i = 0; i < DEPENDENTS - 1; i++) {
            context.write(new IntWritable(i), new DoubleWritable(-inputs[i] * diff));
        }
        context.write(new IntWritable(DEPENDENTS - 1), new DoubleWritable(-1 * diff));
        context.write(new IntWritable(DEPENDENTS), new DoubleWritable(diff * diff));
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < DEPENDENTS - 1; i++) {
            sb.append("\t");
            sb.append("m");
            sb.append(i);
            sb.append(": ");
            sb.append(vals[i]);
            sb.append("\n");
        }
        sb.append(String.format("\tb: %f\n", vals[DEPENDENTS - 1]));
        return sb.toString();
    }
}

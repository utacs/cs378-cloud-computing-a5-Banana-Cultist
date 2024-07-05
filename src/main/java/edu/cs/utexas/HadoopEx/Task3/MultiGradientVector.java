package edu.cs.utexas.HadoopEx.Task3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.thirdparty.org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;

public class MultiGradientVector {
    public final static int DEPENDENTS = 5;
    float[] vals = new float[5];

    public MultiGradientVector(float[] defaults) {
        vals = defaults;
    }

    public MultiGradientVector(float def) {
        for (int i = 0; i < DEPENDENTS; i++) {
            vals[i] = def;
        }
    }

    public MultiGradientVector(Configuration read) {
        for (int i = 0; i < DEPENDENTS; i++) {
            vals[i] = Float.parseFloat(read.get(i + ""));
        }
    }

    public void writeToConfiguration(Configuration conf) {
        for (int i = 0; i < DEPENDENTS; i++) {
            conf.set(i + "", vals[i] + "");
        }
    }
}

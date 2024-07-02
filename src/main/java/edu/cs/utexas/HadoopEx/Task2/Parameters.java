package edu.cs.utexas.HadoopEx.Task2;

import java.util.ArrayList;

public class Parameters {

    public static ArrayList<Double> parameters;
    public static double learning_rate = 0.001;

    public static void initializeParameters(int n, double lr) {
        parameters = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            parameters.add(0.0);
        }
        learning_rate = lr;
    }

    public static void updateParameter(int index, Double value) throws Exception {
        Double curr_val = parameters.get(index);
        parameters.set(index, curr_val - learning_rate * value);
    }

    public static ArrayList<Double> getParameters() {
        return parameters;
    }
}

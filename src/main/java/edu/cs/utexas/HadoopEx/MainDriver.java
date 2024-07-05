package edu.cs.utexas.HadoopEx;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import edu.cs.utexas.HadoopEx.Task1.LinearRegressionDriver;
import edu.cs.utexas.HadoopEx.Task2.GradientDescentDriver;
import edu.cs.utexas.HadoopEx.Task3.MultiGradientDriver;

public class MainDriver extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        int res = 0;
        if (args[1].equals("Task1")) {
            LinearRegressionDriver.main(new String[] {args[0], args[2], args[3]});
        } else if (args[1].equals("Task2")) {
            res = ToolRunner.run(new Configuration(), new GradientDescentDriver(), new String[] {args[0], args[2]});
        } else if (args[1].equals("Task3")) {
            res = ToolRunner.run(new Configuration(), new MultiGradientDriver(), new String[] {args[0], args[2]});
        } else if (args[1].equals("AllTasks")) {
            res = ToolRunner.run(new Configuration(), new LinearRegressionDriver(), new String[] {args[0], args[2]});
            res = ToolRunner.run(new Configuration(), new GradientDescentDriver(), new String[] {args[0], args[3], args[4]});
            res = ToolRunner.run(new Configuration(), new MultiGradientDriver(), new String[] {args[0], args[5], args[6]});
        }
        System.exit(res);
	}

    //This method does nothing
    public int run(String[] args) {
        return 0;
    }
}

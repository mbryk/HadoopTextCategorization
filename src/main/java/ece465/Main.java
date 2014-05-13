package ece465;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Main extends Configured implements Tool {

    public String timeStamp;

    public int featurizeTrainingData(String inputDir, String outputDir) throws InterruptedException, IOException, ClassNotFoundException {
        Job jobWC_train = new Job(getConf());
        jobWC_train.setJarByClass(Main.class);
                
        jobWC_train.setJobName("WordCount");

        jobWC_train.setInputFormatClass(InputFormatWC.class);
        jobWC_train.setOutputKeyClass(WordFile.class);
        jobWC_train.setOutputValueClass(IntWritable.class);

        jobWC_train.setMapperClass(MapClassWC.class); // Map to [wordID 1]
        jobWC_train.setCombinerClass(IntSumReducer.class);
        jobWC_train.setReducerClass(IntSumReducer.class);

        try {
            FileInputFormat.addInputPaths(jobWC_train, inputDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(jobWC_train, new Path(outputDir));

        return jobWC_train.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {

        Job jobKNN;
        Configuration conf = new Configuration();
        jobKNN = new Job(conf);

        String trainingFeatureInputDir;
        String trainedData;
        String testFile;
        String outDir;
        
        timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

        if (args[0].compareTo("--featurize") == 0 || args[0].compareTo("-f") == 0){
            trainingFeatureInputDir = args[1];
            outDir = args[2];

            featurizeTrainingData(trainingFeatureInputDir, outDir);

            return 0;
        }
        else if (args[0].compareTo("--trained") == 0|| args[0].compareTo("-t") == 0) {
            trainedData = args[1];
            testFile = args[2];
            outDir = args[3];

            FileInputFormat.addInputPaths(jobKNN, trainedData);
        }
        else{
            trainingFeatureInputDir = args[0];
            testFile = args[1];
            outDir = args[2];

            featurizeTrainingData(trainingFeatureInputDir, "/tmp/trainingData" + timeStamp);
            FileInputFormat.addInputPaths(jobKNN, "/tmp/trainingData" + timeStamp + "/part-r-00000");
        }


        jobKNN.setJarByClass(Main.class);
        jobKNN.setJobName("KNN");
        jobKNN.setInputFormatClass(InputFormatKNN.class);

        jobKNN.setOutputKeyClass(DoubleWritable.class);
        jobKNN.setOutputValueClass(IntWritable.class);
        
        jobKNN.setMapperClass(MapClassKNN.class);
        jobKNN.setReducerClass(ReduceClassKNN.class);

        FileOutputFormat.setOutputPath(jobKNN, new Path(outDir));

        return jobKNN.waitForCompletion(true) ? 0 : 1;

    }


    public String featurizeTestData(String inputPath) throws IOException {
        
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Main(), args);
        System.exit(ret);
    }
}

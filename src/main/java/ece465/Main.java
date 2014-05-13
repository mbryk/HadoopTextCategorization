package ece465;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class Main extends Configured implements Tool {

    public String timeStamp;

    public int featurizeTrainingData(String inputDir, String outputDir){
        Job jobWC_train = new Job(getConf());
        jobWC_train.setJarByClass(Main.class);
                
        jobWC_train.setJobName("WordCount");

        jobWC_train.setInputFormatClass(InputFormatWC.class);
        jobWC_train.setOutputKeyClass(WordFile.class);
        jobWC_train.setOutputValueClass(IntWritable.class);

        jobWC_train.setMapperClass(MapClassWC.class); // Map to [wordID 1]
        jobWC_train.setCombinerClass(IntSumReducer.class);
        jobWC_train.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPaths(jobWC_train, inputDir);
        FileOutputFormat.setOutputPath(jobWC_train, new Path(outputDir));

        return jobWC_train.waitForCompletion(true) ? 0 : 1;
    }

    public int run(String[] args) throws Exception {
        //args[0] is training files
        //args[1] is test file
        //args[2] is output file

        String trainingFeatureInputDir;
        String trainedData;
        String testFile;
        String outDir;
        
        timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());

        if (args[0].compareTo("--featurize") || args[0].compareTo("-f") ){
            trainingFeatureInputDir = args[1];
            outDir = args[2];

            featurizeTrainingData(trainingFeatureInputDir, outDir);

            return 0;
        }
        else if (args[0].compareTo("--trained") || args[0].compareTo("-t")) {
            trainedData = args[1];
            testFile = args[2];
            outDir = args[3];
        }
        else{
            trainingFeatureInputDir = args[0];
            testFile = args[1];
            outDir = args[2];

            featurizeTrainingData(trainingFeatureInputDir, "/tmp/trainingData" + timeStamp);
        }

        Job jobKNN;
        jobKNN = new Job(getConf());
        jobKNN.setJarByClass(Main.class);
        jobKNN.setJobName("KNN");
        jobKNN.setInputFormatClass(InputFormatKNN.class);

        jobKNN.setOutputKeyClass(IntWritable.class);
        jobKNN.setOutputValueClass(MapOutputKNN.class);
        
        jobKNN.setMapperClass(MapClassKNN.class);
        jobKNN.setReducerClass(ReduceClassKNN.class);

        FileInputFormat.addInputPaths(jobKNN, "/tmp/trainingData" + timeStamp + "/part-r-00000");
        FileOutputFormat.setOutputPath(jobKNN, new Path(args[2]));

        return jobKNN.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Main(), args);
        System.exit(ret);
    }
}

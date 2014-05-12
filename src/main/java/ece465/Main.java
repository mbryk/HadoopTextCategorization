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

    public int run(String[] args) throws Exception {

        String timeStamp = new SimpleDateFormat("yyyyMMdd_HHmmss").format(Calendar.getInstance().getTime());


        Job jobWC = new Job(getConf());
        jobWC.setJarByClass(Main.class);
                
        jobWC.setJobName("WordCount");

        jobWC.setInputFormatClass(InputFormatWC.class);
        // the keys are words (strings), the values are counts (ints)
        //jobWC.setOutputKeyClass(Text.class);
        jobWC.setOutputKeyClass(WordFile.class);
        jobWC.setOutputValueClass(IntWritable.class);

        jobWC.setMapperClass(MapClassWC.class); // Map to [wordID 1]
        jobWC.setCombinerClass(IntSumReducer.class);
        jobWC.setReducerClass(IntSumReducer.class);

        FileInputFormat.addInputPaths(jobWC, args[0]);
        FileOutputFormat.setOutputPath(jobWC, new Path("/tmp/BookData" + timeStamp));

        if(jobWC.waitForCompletion(true) != true)
            return -1;


//        if(dontHaveTrainingData){
//            jobWC.runOnTrainingData -> trainingfile.txt
//        }
        //jobWC.runOnTestData -> testfile.txt

        // this will output [ID1word=C, ID1word2=C];
        // Now, input this long vector into jobKNN

        //jobKNN will run on trainingfile.txt

        Job jobKNN;
        jobKNN = new Job(getConf());
        jobKNN.setJarByClass(Main.class);
        jobKNN.setJobName("KNN");
        jobKNN.setInputFormatClass(InputFormatKNN.class);

        jobKNN.setOutputKeyClass(IntWritable.class);
        jobKNN.setOutputValueClass(MapOutputKNN.class);
        
        jobKNN.setMapperClass(MapClassKNN.class);
        jobKNN.setReducerClass(ReduceClassKNN.class);

        FileInputFormat.addInputPaths(jobKNN, "/tmp/BookData" + timeStamp + "/part-r-00000");
        FileOutputFormat.setOutputPath(jobKNN, new Path(args[1]));

        return jobKNN.waitForCompletion(true) ? 0 : 1;

    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Main(), args);
        System.exit(ret);
    }
}

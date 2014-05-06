package ece465;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.LineReader;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Job jobWC = new Job(getConf());
        //jobWC.setJarByClass(MultiFileWordCount.class);
                
        jobWC.setJobName("WordCount");

        jobWC.setInputFormatClass(InputFormatWC.class);
        // the keys are words (strings), the values are counts (ints)
        jobWC.setOutputKeyClass(Text.class);
        jobWC.setOutputValueClass(IntWritable.class);

        jobWC.setMapperClass(MapClassWC.class);
        jobWC.setCombinerClass(IntSumReducer.class);
        jobWC.setReducerClass(IntSumReducer.class);

        // this will output [ID1word=C, ID1word2=C];
        // Now, input this long vector into jobKNN

        Job jobKNN = new Job(getConf());
        jobKNN.setJobName("KNN");
        //set the InputFormat of the job to our InputFormat
        jobKNN.setInputFormatClass(InputFormatKNN.class);

        jobKNN.setOutputKeyClass(IntWritable.class);
        jobKNN.setOutputValueClass(IntWritable.class);
        
        jobKNN.setMapperClass(MapClassKNN.class);
        jobKNN.setReducerClass(ReduceClassKNN.class);

        FileInputFormat.addInputPaths(job, args[0]);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : 1;


    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Main(), args);
        System.exit(ret);
    }
}

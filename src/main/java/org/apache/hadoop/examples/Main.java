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

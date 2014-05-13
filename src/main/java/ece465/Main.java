package ece465;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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

        String trainingFeatureInputDir;
        String trainedData;
        String testFile;
        String outDir;
        String labelsFile;
        
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
            labelsFile = args[4];
        }
        else{
            trainingFeatureInputDir = args[0];
            testFile = args[1];
            outDir = args[2];
            labelsFile = args[3];
            trainedData = "/tmp/trainingData" + timeStamp + "/part-r-00000";

            featurizeTrainingData(trainingFeatureInputDir, "/tmp/trainingData" + timeStamp);
        }


        Job jobKNN;
        Configuration conf = new Configuration();
        String testCases = featurizeTestData(testFile);
        conf.set("test_cases", testCases);
        jobKNN = new Job(conf);
        jobKNN.setJarByClass(Main.class);
        jobKNN.setJobName("KNN");
        jobKNN.setInputFormatClass(InputFormatKNN.class);
		jobKNN.setMapOutputValueClass(MapOutputKNN.class);

        jobKNN.setOutputKeyClass(Text.class);
        jobKNN.setOutputValueClass(Text.class);
        
        jobKNN.setMapperClass(MapClassKNN.class);
        jobKNN.setReducerClass(ReduceClassKNN.class);

        FileInputFormat.addInputPaths(jobKNN, trainedData);
        FileOutputFormat.setOutputPath(jobKNN, new Path(outDir));

        if(jobKNN.waitForCompletion(true) != true)
             return -1;


        Map<Integer, String> classLabels = new HashMap<Integer, String>();
        FileReader fileReader = new FileReader(labelsFile);
        BufferedReader reader = new BufferedReader(fileReader);
        String line;
        while ((line = reader.readLine()) != null) {
            String[] cl = line.split("\\s");
            String category = cl[1];
            String[] file =cl[0].split("\\.");
            String[] f = file[file.length -2].split("/");
            String myFile_S = f[f.length - 1];
            Integer myFile = Integer.parseInt(myFile_S);
            classLabels.put(myFile, category);
        }


        int k = 5;

        Configuration config = new Configuration();
        FileSystem hdfs = FileSystem.get(config);

        Path srcPath = new Path(outDir + "/part-r-00000");
        Path dstPath = new Path("/tmp/" + timeStamp);

        hdfs.copyToLocalFile(srcPath, dstPath);

        fileReader =  new FileReader("/tmp/" + timeStamp);
        reader = new BufferedReader(fileReader);

		//reader.readLine(); // Read the key.
 		PrintWriter printWriter = new PrintWriter("/tmp/textClassificationOutput2.txt");

		while( (line=reader.readLine()) != null){
			String testID = line;
			Map<String, Integer> knnCounts = new HashMap<String, Integer>();
			for(int j=0;j<k;j++){
				line = reader.readLine();
				String curLabel = classLabels.get(Integer.parseInt(line));
				if (knnCounts.containsKey(curLabel))
	                knnCounts.put(curLabel, knnCounts.get(curLabel)+1);
	            else
	                knnCounts.put(curLabel, 1);
			}
			int max = 0;
	        String answer = null;
	        for (Map.Entry<String, Integer> entry : knnCounts.entrySet()) {
	            if (entry.getValue() > max){
	                max = entry.getValue();
	                answer = entry.getKey();
	            }
	        }
			String str = "./corpus2/train/"+testID.replaceAll("\\s","") + " " +answer;
			printWriter.println(str);
		}
		printWriter.close();
        return 0;
    }

    public String featurizeTestData(String inputDir) throws IOException {
		String output = "";
		int i = 0;
		File dir = new File(inputDir);
		for (File f : dir.listFiles()) {
		    FileReader fileReader = new FileReader(f);
		    BufferedReader reader = new BufferedReader(fileReader);
		    String line;
		    Map<String, Integer> hashMap= new HashMap<String, Integer>();
		    while ((line = reader.readLine()) != null) {
		        line = line.replaceAll("[\"(){},.;!?<>%:=]", "");
		        String[] words = line.split("\\s");
		        for (String word : words) {
		            if (hashMap.containsKey(word)) {
		                hashMap.put(word, hashMap.get(word) + 1);
		            } else {
		                hashMap.put(word, 1);
		            }
		        }
		    }
			String name = f.getName().replaceAll("[\"(){},!?<>%:]","");
		    output += name + ":";
		    for (Map.Entry<String, Integer> entry : hashMap.entrySet()) {
		        output += entry.getKey() + "=" + entry.getValue() + ";";
		    }
			output += ",";
			i++;
		}
		//System.out.println(output);
        return output;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Main(), args);
        System.exit(ret);
    }
}

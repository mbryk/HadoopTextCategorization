package ece465;

/* 
This will get one training case from the trainingWC file as explained in InputFormatKNN.java, followed by all of the test cases.
It will compare the training case to each of the testCases, and output {testCase, [class,distance]}
All of the cases will be period delimited. The word counts will be ";"-delimited, where "word=count".
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class MapClassKNN extends
	Mapper<Text, Text, DoubleWritable, IntWritable> {

	private Map<String,Integer> wc;
	//private int trainLength = 0;

	private final static IntWritable one
		= new IntWritable(1);

	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException{
		
		wc = new HashMap<String,Integer>();
		int trainLength = 0;

        if (key.getLength() == 0){
            return;
        }
        String s[] = key.toString().split("\\.");
		int category = Integer.valueOf(s[0]);

		// Parse Training Case
		StringTokenizer words = new StringTokenizer(value.toString(),";");

		while(words.hasMoreTokens()){
			String wordcount[] = words.nextToken().split("=");
			int trainVal = Integer.parseInt(wordcount[1].replaceAll("\\s","")); // strips out whitespace around number
			wc.put(wordcount[0],trainVal);
			trainLength += Math.pow(trainVal, 2);
		}

        Configuration configuration = context.getConfiguration();
        String test = configuration.get("test_cases");
        StringTokenizer test_cases = new StringTokenizer(test, ".");

		// Map Each Test Case
		while(test_cases.hasMoreTokens()){
//			MapOutputKNN catSim = new MapOutputKNN(category);
            DoubleWritable sim = new DoubleWritable(similarity(test_cases.nextToken(), trainLength));
            IntWritable cat = new IntWritable(category);
			// Assuming there is only one test case: testCase id = 1
			context.write(sim, cat);
			//context.write(testcase, catSim);
		}
	}

	private double similarity(String testCase, int trainLength){
		StringTokenizer words = new StringTokenizer(testCase,";");		
		double testLength = 0;
		double cross = 0;
		while(words.hasMoreTokens()){
            String word = words.nextToken();
			String wordcount[] = word.split("=");

			int trainVal = 0;
            if (wc.containsKey(wordcount[0])){
                trainVal = wc.get(wordcount[0]);
            }
			int testVal = Integer.parseInt(wordcount[1]);

			cross += trainVal*testVal;
			testLength += Math.pow(testVal, 2);
		}
		return -cross/(Math.sqrt(testLength)*Math.sqrt(trainLength));
		//return Math.sqrt(trainLength);
	}
}

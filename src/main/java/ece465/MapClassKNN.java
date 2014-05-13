package ece465;

/* 
This will get one training case from the trainingWC file as explained in InputFormatKNN.java, followed by all of the test cases.
It will compare the training case to each of the testCases, and output {testCase, [class,distance]}
All of the cases will be period delimited. The word counts will be ";"-delimited, where "word=count".
*/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class MapClassKNN extends
	Mapper<Text, Text, IntWritable, MapOutputKNN> {

	private Map<String,Integer> wc;
	private int trainLength = 0;

	private final static IntWritable one
		= new IntWritable(1);

	public void map(Text key, Text value, Context context)
		throws IOException, InterruptedException{
		
		wc = new HashMap<String,Integer>();

        if (key.getLength() == 0){
            return;
        }
        String s[] = key.toString().split("\\.");
		int category = Integer.valueOf(s[0]);

		String line = value.toString();
		StringTokenizer cases = new StringTokenizer(line,".");

		// Parse Training Case
		StringTokenizer words = new StringTokenizer(cases.nextToken(),";");

		while(words.hasMoreTokens()){
			String wordcount[] = words.nextToken().split("=");
			int trainVal = Integer.parseInt(wordcount[1].replaceAll("\\s",""));
			wc.put(wordcount[0],trainVal);
			trainLength += trainVal^2;
		}

		// Map Each Test Case
		while(cases.hasMoreTokens()){
			int sim = similarity(cases.nextToken());
			MapOutputKNN catSim = new MapOutputKNN(category, sim);

			// Assuming there is only one test case: testCase id = 1
			context.write(one,catSim);
			//context.write(testcase, catSim);
		}
	}

	private int similarity(String testCase){
		StringTokenizer words = new StringTokenizer(testCase,";");		
		int testLength = 0;
		int cross = 0;
		while(words.hasMoreTokens()){
			String wordcount[] = words.nextToken().split("=");
			int trainVal = wc.get(wordcount[0]);
			int testVal = Integer.parseInt(wordcount[1]);

			cross += trainVal*testVal;
			testLength += testVal^2;
		}
		return cross/(testLength*trainLength);
	}
}

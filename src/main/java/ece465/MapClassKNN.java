package ece465;

/* 
This will get one training case from the trainingWC file as explained in InputFormatKNN.java, followed by all of the test cases.
It will compare the training case to each of the testCases, and output {testCase, [class,distance]}
All of the cases will be period delimited. The word counts will be ";"-delimited, where "word=count".
*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class MapClassKNN extends
	Mapper<Text, Text, Text, MapOutputKNN> {

	private Map<String,Integer> wc;
	//private int trainLength = 0;

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
        StringTokenizer test_cases = new StringTokenizer(test, ",");

		// Map Each Test Case
		Text testName;
		String[] test_case;
		while(test_cases.hasMoreTokens()){
			test_case = test_cases.nextToken().split(":",2);
			testName = new Text(test_case[0]);
			MapOutputKNN catSim = new MapOutputKNN(category,similarity(test_case[1],trainLength));
			context.write(testName, catSim);
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
		return cross/(Math.sqrt(testLength)*Math.sqrt(trainLength));
		//return Math.sqrt(trainLength);
	}
}

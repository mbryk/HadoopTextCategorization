package ece465;

/* 
This will get one training case from the trainingWC file as explained in InputFormatKNN.java, followed by all of the test cases.
It will compare the training case to each of the testCases, and output {testCase, [class,distance]}
All of the cases will be period delimited. The word counts will be ";"-delimited, where "word=count".
*/

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class MapClassKNN extends
	Mapper<CaseOffset, Text, WCTokenizer, MapOutputKNN> {

	public void map(WordOffset key, Text value, Context context)
		throws IOException, InterruptedException{

		String line = value.toString();
		StringTokenizer cases = new StringTokenizer(line,".");

		// Parse Training Case
		StringTokenizer words = new StringTokenizer(cases.nextToken(),";");		
		while(words.hasMoreTokens()){ 
			wordcount = words.nextToken().split("=");
			trainVal = Integer.parseInt(wordcount[1]);
			wc.add(wordcount[0],trainVal);
			length += trainVal^2;
		}

		// Map Each Test Case
		while(cases.hasMoreTokens()){
			sim = similarity(cases.nextToken());
			MapOutputKNN classSim = new MapOutputKNN(key, sim);
			context.write(testcase, classSim);
		}
	}

	private int similarity(String testCase){
		StringTokenizer words = new StringTokenizer(testCase,";");		
		int testLength = 0;
		while(words.hasMoreTokens()){
			wordcount = words.nextToken().split("=");
			trainVal = wc.get(wordcount[0]);
			testVal = Integer.parseInt(wordcount[1]);

			num += trainVal*testVal;
			testLength += testVal^2;
		}
		return num/(testLength*trainLength);
	}
}

package ece465;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class MapClassWC extends
	Mapper<WordOffset, Text, WordFile, IntWritable> {

	private final static IntWritable one
		= new IntWritable(1);
	//private WordFile wf;

	public void map(WordOffset key, Text value, Context context)
		throws IOException, InterruptedException{

		String line = value.toString();
		line = line.replaceAll("[\"(){},.;!?<>%]", "");
		StringTokenizer itr = new StringTokenizer(line);
		while (itr.hasMoreTokens()){
			WordFile wf = new WordFile(key.fileName, itr.nextToken());
			context.write(wf, one);
		}

	}
}

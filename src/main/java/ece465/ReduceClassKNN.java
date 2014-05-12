package ece465;
/*
The Reducer gets key=testCaseID, value=iterable array of [class,similarity].
It loops through this array, and finds the class with the highest similarity.
It returns key=testCaseID,value=class
*/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceClassKNN extends Reducer<IntWritable, MapOutputKNN, IntWritable, IntWritable> {
        public void reduce(IntWritable key, Iterable<MapOutputKNN> values, Context context) throws IOException, InterruptedException {

            IntWritable category = new IntWritable();
           
			int max = -1;
			int tmpcat = -1;
 
            for (MapOutputKNN val : values) {
				if(val.similarity>max){
					max = val.similarity;
					tmpcat = val.category;
				}
			}

			category.set(tmpcat);
            context.write(key, category);
        }
}

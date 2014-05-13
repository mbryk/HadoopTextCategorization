package ece465;
/*
The Reducer gets key=testCaseID, value=iterable array of [class,similarity].
It loops through this array, and finds the class with the highest similarity.
It returns key=testCaseID,value=class
*/

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReduceClassKNN extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            IntWritable category = new IntWritable();
           
			int max = -1;
			int tmpcat = -1;
 
            for (IntWritable val : values) {
//				if(val.similarity>max){
//					max = val.similarity;
//					tmpcat = val.category;
//				}
                context.write(key, val);
			}

//			category.set(tmpcat);
//            context.write(key, category);
        }
}

package ece465;
/*
The Reducer gets key=testCaseID, value=iterable array of [class,similarity].
It loops through this array, and finds the class with the highest similarity.
It returns key=testCaseID,value=class
*/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Comparator;
import java.util.ArrayList;

public class ReduceClassKNN extends Reducer<IntWritable, MapOutputKNN, IntWritable, Text> {
		public static int limit = 5;
        public void reduce(IntWritable key, Iterable<MapOutputKNN> values, Context context) throws IOException, InterruptedException {
			Queue<MapOutputKNN> topCategories = new PriorityQueue<MapOutputKNN>(limit+1, similarityCompare);    

            for (MapOutputKNN val : values) {
				topCategories.add(val.clone()); // This clone is necessary. Otherwise, it overwrites the reference which is in the queue.
        		if(topCategories.size() > limit){
					topCategories.poll(); // Takes out smallest value in queue. Leaves in top 5.
	        	}
			}

			String output = "";
			while(true){
				MapOutputKNN cs = topCategories.poll();
				if(cs==null) break;
				output = "\n" + cs.category + output;
			}
			context.write(key,new Text(output));
        }

		//Comparator anonymous class implementation
		public static Comparator<MapOutputKNN> similarityCompare = new Comparator<MapOutputKNN>(){
		    @Override
		    public int compare(MapOutputKNN cs1, MapOutputKNN cs2) {
		        return (cs1.similarity > cs2.similarity) ? 1 : -1;
		    }
		};
}

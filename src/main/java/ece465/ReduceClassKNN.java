package ece465;
/*
The Reducer gets key=testCaseID, value=iterable array of [class,similarity].
It loops through this array, and finds the class with the highest similarity.
It returns key=testCaseID,value=class
*/

import org.apache.hadoop.mapreduce.Reducer;

public static class ReduceClassKNN extends Reducer<IntWritable, Text, LongWritable, Text> {
        public void reduce(IntWritable key, Iterable<MapOutputKNN> values, Context context) throws IOException, InterruptedException {

            IntWritable tclass = new IntWritable();
           
			int max = null;
			int tmpclass = null;
 
            for (MapOutputKNN val : values) {
				if(max ==null || val.sim>max){
					max = val.sim;
					tmpclass = val.tclass;
				}
			}

			tclass.set(tmpclass);
            context.write(key, tclass);
            tclass.clear();
        }
}

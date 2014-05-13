package ece465;

/* MapOutputKNN.java

Defines the tuple for the value returned from the KNN Mapper: [class, similarity].

*/

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MapOutputKNN implements Writable{

	public int category;
	public double similarity;

	public MapOutputKNN(int cat, double sim){
		category = cat;
		similarity = sim;
	}

	public MapOutputKNN(){
		this(0, 0);
	}

	public void write(DataOutput out) throws IOException{
		out.writeInt(category);
		out.writeDouble(similarity);
	}

	public void readFields(DataInput in) throws IOException{
		category = in.readInt();
		similarity = in.readDouble();
	}

	public String toString(){
		return this.category + "," + this.similarity + ",";
	}

	public MapOutputKNN clone(){
		return new MapOutputKNN(category,similarity);
	}
		
}

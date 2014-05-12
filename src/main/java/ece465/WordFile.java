package ece465;

/* WordFile.java

Defines the tuple for the key returned from the WC Mapper
and the WC Reducer: [word, fileName].

*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class WordFile implements WritableComparable{

	public String word;
	public String fileName;

	public WordFile(String fn, String w){
		this.word = w;
		this.fileName = fn;
	}

	public WordFile(){
		this(null, null);
	}

	public void write(DataOutput out) throws IOException{
		Text.writeString(out, word);
		Text.writeString(out, fileName);
	}

	public void readFields(DataInput in) throws IOException{
		this.word = Text.readString(in);
		this.fileName = Text.readString(in);
	}

	public String toString(){
		return this.fileName + "," + this.word + ",";
	}

	public int compareTo(Object o){
		WordFile other = (WordFile)o;
		String str1 = this.fileName + this.word;
		String str2 = other.fileName + other.word;
		return str1.compareTo(str2);
	}

	public boolean equals(Object o){
		if(o instanceof WordFile)
			return this.compareTo(o) == 0;
		else
			return false;
	}

	public int hashCode(){
		String str = this.word + this.fileName;
		return str.hashCode();
	}

}

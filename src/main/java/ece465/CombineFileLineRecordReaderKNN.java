package ece465;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class CombineFileLineRecordReaderKNN
	extends RecordReader<Text, Text>{
	
	private long startOffset; //offset of the chunk;
	private long end; //end of the chunk;
	private long pos; // current pos 
	private FileSystem fs;
	private Path path;
	private Text key;
	private Text value;

	private FSDataInputStream fileIn;
	private LineReader reader;

	Text curLine_T = new Text();
	String curLine_S = new String();
	//String value_S = new String();
	String curID = "NF";
	boolean used = false;
	boolean haveLine = false;
	String[] splits = {"n1", "n2"};

	public CombineFileLineRecordReaderKNN(CombineFileSplit split,
		TaskAttemptContext context, Integer index) throws IOException {

		this.path = split.getPath(index);
		fs = this.path.getFileSystem(context.getConfiguration());
		this.startOffset = split.getOffset(index);
		this.end = startOffset + split.getLength(index);
		boolean skipFirstLine = false;

		//open the file
		fileIn = fs.open(path);
		if (startOffset != 0) {
			skipFirstLine = true;
			--startOffset;
			fileIn.seek(startOffset);
		}
		reader = new LineReader(fileIn);
		if (skipFirstLine) {  // skip first line and re-establish "startOffset".
			startOffset += reader.readLine(new Text(), 0,
		            (int)Math.min((long)Integer.MAX_VALUE, end - startOffset));
		}
		this.pos = startOffset;
	}

    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
    }

    public void close() throws IOException { }

    public float getProgress() throws IOException {
		if (startOffset == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - startOffset) / (float)(end - startOffset));
		}
    }

    public boolean nextKeyValue() throws IOException {
		if (key == null) {
			key = new Text();
		}
		if (value == null) {
			value = new Text();
		}
		String value_S = value.toString();
		int newSize = 0;
		while (pos < end){
			if (!haveLine){
				newSize = reader.readLine(curLine_T);
				if (newSize == 0){
					key = null;
					value = null;
					return false;
				}
				curLine_S = curLine_T.toString();
				splits = curLine_S.split(",");
				pos += newSize;
			}
			else{
				value = null;
				value = new Text();
				value_S = new String();
				haveLine = false;
			}
			if (splits[0].compareTo(curID) == 0){
				value_S = value_S.concat(splits[1]);
				value_S = value_S + "=" + splits[2] + ";";
			}
			else{
				if (curID.compareTo("NF") != 0){
					key.set(curID);
					value.set(value_S);
				}

				curID = splits[0];
				haveLine = true;

				if (curID.compareTo("NF") != 0)
					return true;
			}
		}
		if (used == false){
			key.set(curID);
			value.set(value_S);
			used = true;
			return true;
		}		
		else{
			key = null;
			value = null;
			return false;
		}
    }

    public Text getCurrentKey() 
        throws IOException, InterruptedException {
		return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
    }

}
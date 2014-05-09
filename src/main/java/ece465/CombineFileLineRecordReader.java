package ece465;

/* CombineFileLineRecordReader.java

Defines for the K/V pair is read into the WC Mapper, reads
in <WordOffset, Text>.

WordOffset is defined in WordOffset.java, and will
give the Mapper both the file name  and the line in
the file.

*/

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.util.LineReader;


public class CombineFileLineRecordReader 
	extends RecordReader<WordOffset, Text> {

	private long startOffset; //offset of the chunk;
	private long end; //end of the chunk;
	private long pos; // current pos 
	private FileSystem fs;
	private Path path;
	private WordOffset key;
	private Text value;

	private FSDataInputStream fileIn;
	private LineReader reader;

	public CombineFileLineRecordReader(CombineFileSplit split,
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
			key = new WordOffset();
			key.fileName = path.getName();
		}
		key.offset = pos;
		if (value == null) {
			value = new Text();
		}
		int newSize = 0;
		if (pos < end) {
			newSize = reader.readLine(value);
			pos += newSize;
		}
		if (newSize == 0) {
			key = null;
			value = null;
			return false;
		} else {
			return true;
		}
    }

    public WordOffset getCurrentKey() 
        throws IOException, InterruptedException {
		return key;
    }

    public Text getCurrentValue() throws IOException, InterruptedException {
		return value;
    }

}
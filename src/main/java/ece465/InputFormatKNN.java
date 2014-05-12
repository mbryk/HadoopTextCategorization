package ece465;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class InputFormatKNN
	extends CombineFileInputFormat<Text, Text> {

    public RecordReader<Text,Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      return new CombineFileRecordReader<Text, Text>(
        (CombineFileSplit)split, context, CombineFileLineRecordReaderKNN.class);
    }

}

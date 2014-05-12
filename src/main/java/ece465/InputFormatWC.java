package ece465;

/* InputFormatWC.java

The format for input into the WC Mapper: K/V pair of
<WordOffset, Text>.

WordOffset is defined in WordOffset.java, and will
give the Mapper both the file name  and the line in
the file.

CombineFileLineRecordReaderWC.java defines how the keys
and values are read in.

*/

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class InputFormatWC
	extends CombineFileInputFormat<WordOffset, Text> {

    public RecordReader<WordOffset,Text> createRecordReader(InputSplit split,
        TaskAttemptContext context) throws IOException {
      return new CombineFileRecordReader<WordOffset, Text>(
        (CombineFileSplit)split, context, CombineFileLineRecordReaderWC.class);
    }

}

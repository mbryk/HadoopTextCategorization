package ece465;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import ece465.WordFile;

public class OutputFormatWC extends FileOutputFormat<WordFile, IntWritable> {

  protected static class RecordWriterWC<WordFile, IntWritable> 
    implements RecordWriter<WordFile, IntWritable> {
    
    private static final String utf8 = "UTF-8";

    private DataOutputStream out;
    String currentFile;

    public RecordWriterWC(DataOutputStream out) throws IOException {
      this.out = out;
    }

    private void writeFileID(Object o) throws IOException {
      Text textOut = (Text) o;
      out.write(textOut.getBytes(), 0, textOut.getLength());
      out.writeBytes("\t");
    }

    private void writeWord(Object o) throws IOException{
      Text textOut = (Text) o;
      out.write(textOut.getBytes(), 0, textOut.getLength());
      out.writeBytes("=");
    }

    private void writeCount(Object o) throws IOException{
      if (o instanceof IntWritable){
        out.write(o.toString().getBytes(utf8));
        out.writeBytes(";");
      }
    }


    public synchronized void write(WordFile key, IntWritable value) throws IOException {


      if (key instanceof WordFile && value instanceof IntWritable){
        if (key.getFileName().compareTo(currentFile) != 0){
          writeFileID(key.getFileName());
          currentFile = key.getFileName();
        }
        writeWord(key.getWord());
        writeCount(value);
      }
    }

    public synchronized void close(Reporter reporter) throws IOException {
        out.close();
    }
  }

  public RecordWriter<WordFile, IntWritable> getRecordWriter(FileSystem ignored, JobConf job,
      String name, Progressable progress) throws IOException {
    Path file = FileOutputFormat.getTaskOutputPath(job, name);
    FileSystem fs = file.getFileSystem(job);
    FSDataOutputStream fileOut = fs.create(file, progress);
    return new RecordWriterWC<WordFile, IntWritable>(fileOut);
  }
}
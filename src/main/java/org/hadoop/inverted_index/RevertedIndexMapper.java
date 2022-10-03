package org.hadoop.inverted_index;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RevertedIndexMapper extends Mapper<LongWritable, Text,Text,Text> {
    @Override
    protected  void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
        String path=((FileSplit)context.getInputSplit()).getPath().toString();
        int index=path.lastIndexOf("/");
        String filename=path.substring(index+1);
        String[] words=value.toString().split(" ");
        for(String word:words)
            context.write(new Text(word+":"+filename),new Text("1"));
    }
}

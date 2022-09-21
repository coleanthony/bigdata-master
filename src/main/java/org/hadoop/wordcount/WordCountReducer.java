package org.hadoop.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * 这里是MR程序reducer阶段处理的类
 * KEYIN：这是Reducer阶段数据输入key的数据类型，对应Mapper阶段输出key的类型
 * VALUEIN：这是Reducer阶段数据输入value的数据类型，对应Mapper阶段输出value的类型
 * KEYOUT：这是Reducer阶段输出key的数据类型，本案例中，是Text
 * VALUEOUT：这是Reducer阶段输出value的数据类型，本案例中，是IntWritable
 *
 * **/

public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
    @Override
    protected void reduce(Text key,Iterable<IntWritable> values,Reducer<Text,IntWritable,Text,IntWritable>.Context context) throws IOException,InterruptedException{
        int count=0;
        for(IntWritable value:values){
            count+=value.get();
        }
        context.write(key,new IntWritable(count));
    }
}

package org.hadoop.wordcount;
import java.io.IOException;

import  org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

/*
 *Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT>
 *KEYIN：表示mapper阶段数据输入时key的数据类型，读一行数据，返回一行给MR程序
 *这种情况下KEYIN表示每一行的起始偏移量，因此数据类型为Long
 *VALUEIN： 表示mapper阶段数据输入时Value的数据类型，在默认读取数据组件下，VALUEIN表示读取的一行内容，因此为String
 *KEYOUT：表示mapper阶段数据输出时key的数据类型，本案例中输出的Key是单词，因此用String
 *VALUEOUT：表示mapper阶段数据输出是Value的数据类型，本案例中输出值Value为单词出现的次数，因此用Integer
 *使用Hadoop特殊的序列化类型：long -- LongWritable, String -- Text, Integer -- InWritable
 * */

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected  void map(LongWritable key,Text value,Mapper<LongWritable,Text,Text,IntWritable>.Context context)  throws  IOException,InterruptedException{
        String line=value.toString();
        String[] words=line.split(" ");
        for(String word:words)
            context.write(new Text(word),new IntWritable(1));
    }
}

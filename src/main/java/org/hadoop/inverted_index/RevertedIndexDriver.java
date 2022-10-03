package org.hadoop.inverted_index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RevertedIndexDriver {
    private static final String HDFS_URL="hdfs://master:9000";
    public static void main(String[] args) throws Exception {
        // 创建一个job和任务入口
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",HDFS_URL);
        Job job = Job.getInstance(conf);
        job.setJarByClass(RevertedIndexDriver.class); //main方法所在的class
        //指定job的mapper和输出的类型<k2 v2>
        job.setMapperClass(RevertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //引入combiner
        job.setCombinerClass(RevertedIndexCombiner.class);
        //指定job的reducer和输出的类型<k4 v4>
        job.setReducerClass(RevertedIndexReducer.class);
        job.setOutputKeyClass(Text.class); //k4的类型
        job.setOutputValueClass(Text.class); //v4的类型
        //指定job的输入和输出
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //执行job
        job.waitForCompletion(true);
    }
}


package com.xiong.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        //  1. 获取job
        //  1.1. conf中保存的是hadoop的一些默认配置文件和自定义的配置文件, 例如core-default.xml和core-site.xml等等
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2. 设置jar包路径
        job.setJarByClass(WordCountDriver.class);

        // 3. 关联mapper和reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4. 设置map阶段输出的kv的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5. 设置最终输出的kv的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 设置输入路径和输出路径
        String classPath = "G:\\Projects\\IDEAProjects\\learning\\bigdata\\bigdata_01_hadoop\\hadoop\\src\\main\\resources";
        String inputFilePath = classPath + "/input/wordcount";
        String outputFilePath = classPath + "/output/wordcount";
        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));
        // Linux下指定HDFS文件路径
        // FileInputFormat.setInputPaths(job, new Path(args[0]));
        // FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 提交job
        // 7.1. 从这里才开始执行mapreduce程序, 进入到XXXMapper和XXXReducer程序中
        job.waitForCompletion(true);
    }
}

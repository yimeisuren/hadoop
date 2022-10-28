package com.xiong.mapreduce.writable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //todo: 设置之后, 这些保存在job的哪个变量中呢?
        job.setJarByClass(FlowCountDriver.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        //todo: 1. 不要使用绝对路径; 2. 为什么需要使用Long类型的呢?
        String classPath = "G:\\Projects\\IDEAProjects\\learning\\bigdata\\bigdata_01_hadoop\\hadoop\\src\\main\\resources";
        String inputFilePath = classPath + "/input/writable";
        String outputFilePath = classPath + "/output/writable";

        FileInputFormat.setInputPaths(job, new Path(inputFilePath));
        FileOutputFormat.setOutputPath(job, new Path(outputFilePath));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}

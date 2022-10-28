package com.xiong.mapreduce.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private final Text keyOut = new Text();
    private final FlowBean ValueOut = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String s = value.toString();
        String[] fields = s.split("\\s+");

        keyOut.set(fields[1]);


        //设置Bean对象中的属性
        ValueOut.setUpFlow(Long.parseLong(fields[3]));
        ValueOut.setDownFlow(Long.parseLong(fields[4]));

        context.write(keyOut, ValueOut);
    }

}

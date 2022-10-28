package com.xiong.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable valueOut = new IntWritable();

    /**
     * 对于每个key, 调用一个reduce()函数
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        // 经历了shuffle过程后, 会变为key: (1,1,1,1,...,1)的形式, 例如
        // key_1: (1,1) 代表key_1的数据项有2项
        // key_2: (1,1,1) 代表key_2的数据项有3项
        //聚合
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        //保存输出
        valueOut.set(sum);
        context.write(key, valueOut);
    }
}

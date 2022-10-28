package com.xiong.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KEYIN: map阶段输入的key类型
 * <p>
 * VALUEIN: map阶段输入的value类型
 * <p>
 * KEYOUT: map阶段输出的key类型
 * <p>
 * VALUEOUT: map阶段输出的value类型
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //这里一定要先new, 否则会出现空指针
    //Text里面封装了一个字节数组
    private final Text keyOut = new Text();
    private final IntWritable valueOut = new IntWritable(1);

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        super.setup(context);
    }


    /**
     * todo: key在哪里使用到了呢? key是个什么东西呢?
     *
     * @param key     行号?? 自动添加的, 并不是文档中直接出现的
     * @param value
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //map是一行一行处理数据
        //获取文档中的每一行, 对应value, 为了方便字符串的处理, 将其转化为String类型
        String s = value.toString();

        //根据业务编写相关代码
        String[] words = s.split(" ");
        for (String word : words) {
            //相当于进行强制类型转换
            keyOut.set(word);
            //context保存数据和配置环境等, 用于mapper和reducer之间的数据传递
            context.write(keyOut, valueOut);
        }
    }

    @Override
    public void run(Context context)
            throws IOException, InterruptedException {

        super.run(context);
    }
}

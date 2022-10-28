package com.xiong.mapreduce.writable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private final FlowBean ValueOut = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        long sumOfUpFlow = 0L;
        long sumOfDownFlow = 0L;
        for (FlowBean value : values) {
            sumOfUpFlow += value.getUpFlow();
            sumOfDownFlow += value.getDownFlow();
        }

        ValueOut.setUpFlow(sumOfUpFlow);
        ValueOut.setDownFlow(sumOfDownFlow);
        context.write(key, ValueOut);
    }

}

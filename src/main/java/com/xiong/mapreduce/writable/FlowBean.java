package com.xiong.mapreduce.writable;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义Bean对象, 实现序列化的步骤
 * <p>
 * 1. 实现Writable接口
 */

@Data
public class FlowBean implements Writable {
    private long upFlow;
    private long downFlow;
    private long sumFlow;


    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
        setSumFlow();
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
        setSumFlow();
    }

    private void setSumFlow() {
        this.sumFlow = this.downFlow + this.upFlow;
    }


    /**
     * 序列化方法
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     *
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        //序列化和反序列化方法需要对应上, 相当于按类型所在的字节数去读取
        this.upFlow = in.readLong();
        this.downFlow = in.readLong();
        this.sumFlow = in.readLong();
    }


}

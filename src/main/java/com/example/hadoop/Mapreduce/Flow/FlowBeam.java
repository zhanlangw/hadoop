package com.example.hadoop.Mapreduce.Flow;

import lombok.Data;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
public class FlowBeam implements Writable, WritableComparable<FlowBeam> {
    private long upflow;

    private long downflow;
    private long sumFlow;

    public FlowBeam() {
    }

    public FlowBeam(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumFlow = upflow + downflow;
    }

    @Override
    public String toString() {
        return upflow + "\t" + downflow + "\t" + sumFlow + "\t";
    }

    /**
     * 序列化
     *
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upflow);
        dataOutput.writeLong(downflow);
        dataOutput.writeLong(sumFlow);
    }


    /**
     * 反序列化
     *
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }

    //排序，map task 分区的数据默认是按照输出key排序的，
    @Override
    public int compareTo(FlowBeam beam) {
        return this.sumFlow > beam.getSumFlow() ? 1 : -1;
    }
}

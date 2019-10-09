package com.example.hadoop.Mapreduce.Flow;

import lombok.Data;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
@Data
public class FlowBeam implements Writable{
    private long upflow;

    private long downflow;

   public FlowBeam(){}

    public FlowBeam(long upflow, long downflow) {
        this.upflow = upflow;
        this.downflow = downflow;
        this.sumFlow = upflow + downflow;
    }

    private long sumFlow;

    @Override
    public String toString() {
        return "FlowBeam{" +
                "upflow=" + upflow +
                ", downflow=" + downflow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    /**
     * 序列化
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
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upflow = dataInput.readLong();
        this.downflow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();
    }
}

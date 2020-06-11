package com.example.hadoop.Mapreduce.order;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DomainBean implements Writable, WritableComparable<DomainBean> {
    private String orderId;

    private String price;

    private String orderDesc;

    private String productId;

    private String productName;

    private String productDesc;


    //序列化
    @Override
    public void write(DataOutput output) throws IOException {
        output.writeUTF(orderId);
        output.writeUTF(price);
        output.writeUTF(orderDesc);
        output.writeUTF(productId);
        output.writeUTF(productName);
        output.writeUTF(productDesc);
    }

    //反序列化
    @Override
    public void readFields(DataInput input) throws IOException {
        this.orderId = input.readUTF();
        this.price = input.readUTF();
        this.orderDesc = input.readUTF();
        this.productId = input.readUTF();
        this.productName = input.readUTF();
        this.productDesc = input.readUTF();

    }

    @Override
    public int compareTo(DomainBean bean) {
        return Long.parseLong(bean.getPrice()) > Long.parseLong(this.price) ? 1 : -1;
    }

    @Override
    public String toString() {
        return
                orderId + '\t' +
                        price + '\t' +
                        orderDesc + '\t' +
                        productId + '\t' +
                        productName + '\t' +
                        productDesc + '\t';
    }
}


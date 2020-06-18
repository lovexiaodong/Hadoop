package com.zyd.groupingcoparator;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
public class OrderBean implements WritableComparable<OrderBean> {


    private String orderId;
    private String productId;
    private double price;


    public int compareTo(OrderBean o) {

        if(this.orderId.compareTo(o.orderId) == 0){

            return Double.compare(o.getPrice(), this.price);

        }
        return this.orderId.compareTo(o.orderId);
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(orderId);
        out.writeUTF(productId);
        out.writeDouble(price);

    }

    public void readFields(DataInput in) throws IOException {
        this.orderId = in.readUTF();
        this.productId = in.readUTF();
        this.price = in.readDouble();

    }


    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }


    @Override
    public String toString() {
        return this.orderId + "\t" + this.productId + "\t" + this.price;
    }
}

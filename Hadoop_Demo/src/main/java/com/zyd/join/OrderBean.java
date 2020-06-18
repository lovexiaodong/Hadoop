package com.zyd.join;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private String id;
    private String pid;
    private int price;
    private String name;

    public int compareTo(OrderBean o) {

        int compare = pid.compareTo(o.getPid());

        if(compare == 0){
            return o.getName().compareTo(this.name);
        }

        return compare;
    }

    public void write(DataOutput dataOutput) throws IOException {

        dataOutput.writeUTF(id);
        dataOutput.writeUTF(pid);
        dataOutput.writeUTF(name);
        dataOutput.writeInt(price);

    }

    public void readFields(DataInput dataInput) throws IOException {

        this.id = dataInput.readUTF();
        this.pid = dataInput.readUTF();
        this.name = dataInput.readUTF();
        this.price = dataInput.readInt();
    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}

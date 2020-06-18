package com.zyd.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class JoinMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {


    private String fileName;
    private OrderBean bean = new OrderBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        fileName = fileSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context);

        String[] split = value.toString().split("\t");
        if(fileName.contains("order")){
            bean.setId(split[0]);
            bean.setPid(split[1]);
            bean.setPrice(Integer.parseInt(split[2]));
        }else {
            bean.setPid(split[0]);
            bean.setName(split[1]);
        }
    }
}

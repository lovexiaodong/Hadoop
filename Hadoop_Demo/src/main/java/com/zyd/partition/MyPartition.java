package com.zyd.partition;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;


public class MyPartition extends Partitioner<Text, IntWritable> {


    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {

        int hashCode = text.hashCode();
        return hashCode;
    }
}

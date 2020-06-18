package com.zyd.groupingcoparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OderDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(new Configuration());

        job.setJarByClass(OderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReduce.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        job.setGroupingComparatorClass(OrderGroupingComparator.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\Learn\\Hadoop\\input\\order.txt"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\Learn\\Hadoop\\output"));

        boolean res = job.waitForCompletion(true);
        System.exit(res? 1:0);
    }
}

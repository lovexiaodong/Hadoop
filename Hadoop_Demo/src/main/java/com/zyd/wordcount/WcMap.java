package com.zyd.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMap extends Mapper<LongWritable, Text, Text, IntWritable> {

    private Text world  = new Text();
    private IntWritable one = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println("========" + key.get());
        String line = value.toString();
        String[] split = line.split(" ");

        for (String word : split){
            this.world.set(word);
            context.write(this.world, one);
        }
    }
}

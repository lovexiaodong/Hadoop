package com.zyd.inputformat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileFormat extends FileInputFormat<Text, BytesWritable> {


    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }



    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        WholeFileRecordReader wholeFileRecordReader = new WholeFileRecordReader();
        wholeFileRecordReader.initialize(split, context);

        return wholeFileRecordReader;
    }
}

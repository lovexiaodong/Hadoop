package com.zyd.inputformat;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;


//处理一个文件，直接把文件读成key-value
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {

    private boolean notRead = true;
    private Text key = new Text();
    private BytesWritable value = new BytesWritable();

    private FSDataInputStream inputStream;

    FileSplit fileSplit;

    /**
     * 初始化的时候调用
     * @param split
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

         fileSplit = (FileSplit) split;

         Path path = fileSplit.getPath();

        FileSystem fileSystem = path.getFileSystem(context.getConfiguration());
        inputStream = fileSystem.open(path);

    }

    /**
     *  尝试读取下一个Key value 值，如果读到了则返回true，否则false
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {

        if(notRead){

            key.set(fileSplit.getPath().toString());

            byte[] bytes = new byte[(int) fileSplit.getLength()];
            value.set(bytes, 0, bytes.length);
            notRead = false;
            return true;
        }
        return false;
    }

    /**
     * nextKeyValue 为true，表示读到值，独到的值用这个方法获取
     * @return
     * @throws IOException
     * @throws InterruptedException
     */

    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }


    /**
     * 读取进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public float getProgress() throws IOException, InterruptedException {
        return notRead? 0 : 1;
    }

    public void close() throws IOException {

        IOUtils.closeStream(inputStream);
    }
}

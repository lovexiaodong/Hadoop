package com.zyd.join;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class JoinReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {


    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
       //第一个一定是包含name

        Iterator<NullWritable> iterator = values.iterator();
        String name;

        if(iterator.hasNext()){
            //这个地方可能会迷糊，因为key是实时变的，根据迭代器的next，因为next的时候同样会改变key的值
            iterator.next();
            name = key.getName();

            while (iterator.hasNext()){
                key.setName(name);
                context.write(key, NullWritable.get());
            }
        }


    }
}

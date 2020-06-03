package com.zyd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class Client {
    FileSystem fs;
    @Before
    public void before() throws IOException {
         fs = FileSystem.get(URI.create("hdfs://hadoop11:9000"),new Configuration());
    }


    @After
    public void after() throws IOException {
        fs.close();
    }

    @Test
    public void put(){

        try {
//            FileSystem fs = FileSystem.get(URI.create("hdfs://hadoop11:9000"),new Configuration());
            fs.copyFromLocalFile(new Path("D:\\work\\device-2018-06-26-095847.png"),
                    new Path("/"));



        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void delete() throws IOException {
        fs.delete(new Path(""), true);
    }
    
    @Test
    public void list() throws IOException {
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);

        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus status : fileStatuses){
            if(status.isDirectory()){

            }else{
                System.out.println(status.getPath().getName());
            }
        }

    }
}

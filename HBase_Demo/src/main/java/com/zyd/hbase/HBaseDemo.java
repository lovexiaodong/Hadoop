package com.zyd.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class HBaseDemo {

    public static void main(String[] args) {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","115.159.190.55,118.25.83.45,129.211.4.158");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        Connection conn = null;
        try {
             conn = ConnectionFactory.createConnection(conf);

//            HbaseUtils.createNameSpace(conn.getAdmin(), "ns2");
            System.out.println(conn.getAdmin().tableExists(TableName.valueOf("ns1:user")));

            TableName[] tableNames = conn.getAdmin().listTableNames();
            for (TableName tableName : tableNames){
                System.out.println(tableName.getNameAsString());
            }


//
//            Table user = conn.getTable(TableName.valueOf("ns1:user"));
//            System.out.println(user.toString());

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if(conn != null && !conn.isClosed()){
                try {
                    conn.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }


    }

}

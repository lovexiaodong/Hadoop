package com.zyd.utils;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class HbaseUtils {

    public static void createNameSpace(Admin admin,String nameSpace) throws IOException {
        NamespaceDescriptor descriptor = NamespaceDescriptor.create(nameSpace).build();
        admin.createNamespace(descriptor);
    }
    
    public void scanTable(Table table){
        Scan scan = new Scan();
        try {
            ResultScanner scanner = table.getScanner(scan);
            Iterator<Result> iterator = scanner.iterator();

            while (iterator.hasNext()){
                Result next = iterator.next();
                Cell[] cells = next.rawCells();
                for (Cell cell:cells) {
                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                    System.out.println("columnName = " + columnName + "; value = " + value);
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void getRowKey(String rowKey, Table table){

        Get get = new Get(Bytes.toBytes(rowKey));
        try {
            Result result = table.get(get);
            Cell[] cells = result.rawCells();
            for (Cell cell:cells) {
                String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
                String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                System.out.println("columnName = " + columnName + "; value = " + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void putData(Table table){
        Put put = new Put(Bytes.toBytes("rowkey2"));
        try {
            table.put(put);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

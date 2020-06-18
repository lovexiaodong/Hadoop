package com.zyd.groupingcoparator;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderGroupingComparator extends WritableComparator {

    public  OrderGroupingComparator(){
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
         OrderBean orderBean1 = (OrderBean) a;
         OrderBean orderBean2 = (OrderBean) b;

         return orderBean1.getOrderId().compareTo(orderBean2.getOrderId());
    }
}

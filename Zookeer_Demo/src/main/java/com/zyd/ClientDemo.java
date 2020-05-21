package com.zyd;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ClientDemo {

    public static void main(String[] args)  {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        Date time = calendar.getTime();
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        System.out.println(df.format(time));
        calendar.set(Calendar.HOUR_OF_DAY, 1);
        time = new Date(time.getTime() + 24L * 60 * 60 * 1000);

        System.out.println(df.format(time));

        System.out.println(df.format(Calendar.getInstance().getTime()));
//        ClientDemo demo = new ClientDemo();
//        try {
//            demo.init();
////            demo.createNode();
//            demo.getList();
//            demo.wathchNode();
//
//            try {
//                Thread.sleep(24 * 60 * 60 * 10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        demo.createNode();
    }

    private static CountDownLatch latch = new CountDownLatch(1);
    ZooKeeper zooKeeper;
    public void init() throws IOException {
         zooKeeper = new ZooKeeper("115.159.190.55:2181", 10000, new Watcher() {
             public void process(WatchedEvent event) {
                 System.out.println("====================");
                 System.out.println(event.toString());
                 latch.countDown();


             }
         });

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("---------------");
        System.out.println( zooKeeper.getState());
    }


//    public void createNode(){
//        try {
//            String path = zooKeeper.create("/data1", "zook".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//            System.out.println(path);
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }


    private void wathchNode(){
        try {
            Stat exists = zooKeeper.exists("/data2", new Watcher() {
                public void process(WatchedEvent event) {

                    System.out.println("节点改变" + event.toString());
                }
            });

            System.out.println("exits -------" + exists.toString());

//            List<String> children = zooKeeper.getChildren("/data3", new Watcher() {
//                public void process(WatchedEvent event) {
//                    System.out.println("节点改变2" + event.toString());
//                }
//            });
//
//            System.out.println("getChildren =======" + children.toString());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void getList(){
        try {
            List<String> children = zooKeeper.getChildren("/", true);

//            zooKeeper.exists("/", new Watcher() {
//                public void process(WatchedEvent event) {
//
//                }
//            })
            System.out.println(children.toString());
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
 
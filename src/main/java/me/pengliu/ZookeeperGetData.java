package me.pengliu;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.omg.CORBA.INTERNAL;

import java.util.concurrent.CountDownLatch;

public class ZookeeperGetData implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zookeeper;
    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception {
        String path = "/zk-book";

        // connect to ZK
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZookeeperGetData());
        System.out.println(zookeeper.getState());
        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
        }

        System.out.println("ZooKeeper session established.");

        // create znode
        System.out.println("Creating node " + path);
        zookeeper.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // get data from znode
        System.out.println("Getting data and stat info from " + path);
        System.out.println(new String(zookeeper.getData(path, true, stat)));
        // print stat of znode
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());

        // Make data version changes
        System.out.println("Making change for path " + path);
        zookeeper.setData(path, "123".getBytes(), -1);

        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Receive watched event: " + watchedEvent);
        if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            if (Event.EventType.None == watchedEvent.getType() && null == watchedEvent.getPath()) {
                connectedSemaphore.countDown();
            } else if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                try {

                    System.out.println("In watched event handler method, " +
                            "getting data and stat info from " + watchedEvent.getPath());
                    System.out.println(new String(zookeeper.getData(watchedEvent.getPath(), true, stat)));
                    System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());
                } catch (Exception e) {
                }
            }
        }
    }
}

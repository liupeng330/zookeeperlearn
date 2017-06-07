package me.pengliu;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.concurrent.CountDownLatch;

public class ZookeeperCheckExist implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zookeeper;
    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception {
        String path = "/zk-book";

        // connect to ZK
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZookeeperCheckExist());
        System.out.println(zookeeper.getState());
        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
        }

        System.out.println("ZooKeeper session established.");

        // check exist
        stat = zookeeper.exists(path, true);
        if(stat == null) {
            System.out.println("The znode " + path + " doesn't exist!!");
        }

        // create znode
        System.out.println("Creating node " + path);
        zookeeper.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // check exist
        stat = zookeeper.exists(path, true);

        // get znode
        zookeeper.getData(path, true, null);

        // set znode to a new data
        Stat stat = zookeeper.setData(path, "456".getBytes(), -1);
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());

        // set znode with a correct data version
        Stat stat2 = zookeeper.setData(path, "789".getBytes(), stat.getVersion());
        System.out.println(stat2.getCzxid() + ", " + stat2.getMzxid() + ", " + stat2.getVersion());

        // delete znode
        zookeeper.delete(path, -1);

        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent watchedEvent) {
        try {
            if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                if (Event.EventType.None == watchedEvent.getType() && null == watchedEvent.getPath()) {
                    connectedSemaphore.countDown();
                } else if (Event.EventType.NodeCreated == watchedEvent.getType()) {
                    System.out.println("Node " + watchedEvent.getPath() + " created");
                    // re-register watch event
                    zookeeper.exists(watchedEvent.getPath(), true);
                } else if (Event.EventType.NodeDataChanged == watchedEvent.getType()) {
                    System.out.println("Node " + watchedEvent.getPath() + " data changed");
                    // re-register watch event
                    zookeeper.exists(watchedEvent.getPath(), true);
                } else if (Event.EventType.NodeDeleted == watchedEvent.getType()) {
                    System.out.println("Node " + watchedEvent.getPath() + " deleted");
                    // re-register watch event
                    zookeeper.exists(watchedEvent.getPath(), true);
                }
            }
        } catch (Exception ex) {
        }
    }
}

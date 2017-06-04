package me.pengliu;

import org.apache.zookeeper.*;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperGetChildrenSample implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zookeeper;

    public static void main(String[] args) throws Exception {
        String path = "/zk-book";

        // connect to ZK
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZooKeeperGetChildrenSample());
        System.out.println(zookeeper.getState());
        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
        }

        System.out.println("ZooKeeper session established.");

        // create some znode
        zookeeper.create(path, "test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        zookeeper.create(path + "/c1", "c1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // get child znodes
        List<String> childrenList = zookeeper.getChildren(path, true);
        System.out.println("Got child znode info: " + childrenList);

        // create a new child znode to trigger watcher
        System.out.println("Creating a new child znode to trigger watcher");
        zookeeper.create(path + "/c2", "c2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Receive watched event: " + watchedEvent);
        if (Watcher.Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            if (Event.EventType.None == watchedEvent.getType() && null == watchedEvent.getPath()) {
                connectedSemaphore.countDown();
            }
            else if (Event.EventType.NodeChildrenChanged == watchedEvent.getType()) {
                try {
                    System.out.println("Got node children changed event, will re-invoke getChildren method to get latest info: ");
                    List<String> childrenList = zookeeper.getChildren(watchedEvent.getPath(), true);
                    System.out.println("Got child znode info: " + childrenList);
                }
                catch (Exception e) {}
            }
        }
    }
}

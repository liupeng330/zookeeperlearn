package me.pengliu;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import javax.sound.midi.SysexMessage;
import java.util.concurrent.CountDownLatch;

public class ZookeeperSetData implements Watcher {
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);
    private static ZooKeeper zookeeper;
    private static Stat stat = new Stat();

    public static void main(String[] args) throws Exception{
        String path = "/zk-book";

        // connect to ZK
        zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZookeeperSetData());
        System.out.println(zookeeper.getState());
        try {
            connectedSemaphore.await();
        } catch (InterruptedException e) {
        }

        System.out.println("ZooKeeper session established.");

        // create znode
        System.out.println("Creating node " + path);
        zookeeper.create(path, "123".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        // get znode
        zookeeper.getData(path, true, null);

        // set znode to a new data
        Stat stat = zookeeper.setData(path, "456".getBytes(), -1);
        System.out.println(stat.getCzxid() + ", " + stat.getMzxid() + ", " + stat.getVersion());

        // set znode with a correct data version
        Stat stat2 = zookeeper.setData(path, "789".getBytes(), stat.getVersion());
        System.out.println(stat2.getCzxid() + ", " + stat2.getMzxid() + ", " + stat2.getVersion());

        // set znode with an incorrect data version ,will throw exception
        try{
            zookeeper.setData(path, "012".getBytes(), stat.getVersion());
        } catch (KeeperException e) {
            System.out.println("Error: " + e.code() + ", " + e.getMessage());
        }

        Thread.sleep(Integer.MAX_VALUE);
    }

    public void process(WatchedEvent watchedEvent) {
        if(Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            if(Event.EventType.None == watchedEvent.getType() && null == watchedEvent.getPath()) {
                connectedSemaphore.countDown();
            }
        }
    }
}

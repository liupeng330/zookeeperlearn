package me.pengliu;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

public class ZookeeperConstructorSample implements Watcher{
    private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

    public static void main(String[] args) throws Exception{
        ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZookeeperConstructorSample());
        System.out.println(zookeeper.getState());
        try {
            connectedSemaphore.await();
        }
        catch (InterruptedException e) {}

        System.out.println("ZooKeeper session established.");
    }

    public void process(WatchedEvent watchedEvent) {
        System.out.println("Receive watched event: " + watchedEvent);
        if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
            connectedSemaphore.countDown();
        }
    }
}

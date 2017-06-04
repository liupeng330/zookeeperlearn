package me.pengliu;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

public class ZookeeperCreateNodeSample implements Watcher{
        private static CountDownLatch connectedSemaphore = new CountDownLatch(1);

        public static void main(String[] args) throws Exception{
            // connect to ZK
            ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:2181", 5000, new ZookeeperCreateNodeSample());
            System.out.println(zookeeper.getState());
            try {
                connectedSemaphore.await();
            }
            catch (InterruptedException e) {}

            System.out.println("ZooKeeper session established.");

            // create node on ZK, 临时节点, 同步接口
            String path1 = zookeeper.create("/zk-test-ephemeral", "test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("Success create znode: " + path1);

            // create node on ZK, 临时节点，异步接口
            zookeeper.create("/zk-async-test-ephemeral", "test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                    new IStringCalBack(), "I'm the context");

            // create node on ZK, 同名临时节点，异步接口, 将向异步接口传入错误码
            zookeeper.create("/zk-async-test-ephemeral", "test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL,
                    new IStringCalBack(), "I'm the context");

            // create node on ZK, 顺序临时节点，异步接口
            zookeeper.create("/zk--async-test-ephemeral", "test1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL,
                    new IStringCalBack(), "I'm the context, for EPHEMERAL_SEQUENTIAL");

            Thread.sleep(Integer.MAX_VALUE);
        }

        public void process(WatchedEvent watchedEvent) {
            System.out.println("Receive watched event: " + watchedEvent);
            if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                connectedSemaphore.countDown();
            }
        }
}

class IStringCalBack implements AsyncCallback.StringCallback {
    public void processResult(int rc, String path, Object ctx, String name) {
        System.out.println("Create path result: [" + rc + ", " + path + ", " + ctx + ", real path name: " + name);
    }
}

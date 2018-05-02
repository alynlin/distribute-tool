package com.distribute.lock.zk;

import com.distribute.lock.DistributedLock;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class ZkReentrantLock implements DistributedLock {

    private static final Logger LOGGER = Logger.getLogger(ZkReentrantLock.class);

    /**
     * 所有PERSISTENT锁节点的根位置
     */
    public static final String ROOT_PATH = "/ROOT_LOCK/";

    /**
     * 锁的ID,对应zk一个PERSISTENT节点,下挂EPHEMERAL节点.
     */
    private String path;

    /**
     * zk客户端
     */
    private CuratorFramework client;

    /**
     * zk可重入锁
     */
    private InterProcessMutex interProcessMutex;

    public ZkReentrantLock(CuratorFramework client, String lockId) {
        init(client, lockId);
    }

    public void init(CuratorFramework client, String lockId) {
        this.client = client;
        this.path = ROOT_PATH + lockId;
        interProcessMutex = new InterProcessMutex(client, this.path);
    }


    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {

        try {
            return interProcessMutex.acquire(timeout, unit);
        } catch (Exception e) {
            LOGGER.error(e);
        }
        return false;
    }

    @Override
    public void unlock() {
        try {
            interProcessMutex.release();
        } catch (Exception e) {
            //todo
        }
    }
}

package com.distribute.lock.zk;

import com.distribute.lock.Callback;
import com.distribute.lock.DistributedLockTemplate;
import lombok.Setter;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class ZkLockTemplate implements DistributedLockTemplate {

    private static final Logger LOGGER = Logger.getLogger(ZkLockTemplate.class);

    @Setter
    private String address;
    @Setter
    private int sessionTimeout;
    @Setter
    private int connectionTimeouot;
    private RetryPolicy defaultRetryPolicy = new RetryNTimes(Integer.MAX_VALUE, 3000);

    private CuratorFramework client;

    public ZkLockTemplate(String address) {
        setAddress(address);
        setSessionTimeout(20000);
        setConnectionTimeouot(200);
        client = CuratorFrameworkFactory.builder().connectString(address).sessionTimeoutMs(sessionTimeout)
                .connectionTimeoutMs(connectionTimeouot)
                .retryPolicy(defaultRetryPolicy).zookeeperFactory(new CustomZookeeperFactory()).build();
        client.start();
    }

    public ZkLockTemplate(CuratorFramework client) {
        setClient(client);
    }

    private boolean tryLock(ZkReentrantLock distributedReentrantLock, Long timeout) throws Exception {
        return distributedReentrantLock.tryLock(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Object execute(String lockId, int timeout, Callback callback) {
        ZkReentrantLock distributedReentrantLock = null;
        boolean getLock = false;
        try {
            distributedReentrantLock = new ZkReentrantLock(client, lockId);
            if (tryLock(distributedReentrantLock, new Long(timeout))) {
                getLock = true;
                return callback.onGetLock();
            } else {
                return callback.onTimeout();
            }
        } catch (InterruptedException ex) {
            LOGGER.error(ex.getMessage(), ex);
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            if (getLock) {
                distributedReentrantLock.unlock();
            }
        }
        return null;
    }

    public void setClient(CuratorFramework client) {
        this.client = client;
    }
}

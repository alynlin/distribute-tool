package com.distribute.lock.zk;

import org.apache.curator.CuratorConnectionLossException;
import org.apache.curator.utils.DefaultZookeeperFactory;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomZookeeperFactory extends DefaultZookeeperFactory {

    private static final Logger log = LoggerFactory.getLogger(CustomZookeeperFactory.class);
    private volatile boolean isInit = true;
    private static final String MESSAGE = "A HostProvider may not be empty!";

    @Override
    public ZooKeeper newZooKeeper(String connectString, int sessionTimeout, Watcher watcher, boolean canBeReadOnly)
            throws Exception {

        ZooKeeper zookeeper;
        try {
            zookeeper = new ZooKeeper(connectString, sessionTimeout, watcher, canBeReadOnly);
            isInit = false;
        } catch (IllegalArgumentException e) {
            if (isInit) {
                throw e;
            }
            if (MESSAGE.equals(e.getMessage())) {
                log.warn("ZooKeeper client creation failed for server list: {}", connectString, e.getMessage());
                throw new CuratorConnectionLossException();
            } else {
                throw e;
            }
        }
        return zookeeper;
    }
}

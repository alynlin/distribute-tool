package com.distribute.lock.zk;

import com.distribute.lock.Callback;
import lombok.Setter;
import org.apache.log4j.Logger;
import org.junit.Test;

import static org.junit.Assert.*;

public class ZkLockTemplateTest {

    Logger logger = Logger.getLogger(ZkLockTemplateTest.class);

    @Test
    public void execute() throws Exception {
        String address = "192.168.55.104:2181";


        ZkLockTemplate zkLockTemplate = new ZkLockTemplate(address);

        zkLockTemplate.execute("0001", 1000, new Callback() {

            @Override
            public Object onGetLock() throws InterruptedException {

                logger.info("get lock");
                return null;
            }

            @Override
            public Object onTimeout() throws InterruptedException {

                logger.info("get lock timeoout");
                return null;
            }
        });
    }

}
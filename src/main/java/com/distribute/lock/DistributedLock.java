package com.distribute.lock;

import java.util.concurrent.TimeUnit;

public interface DistributedLock {

    boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException;

    void unlock();
}

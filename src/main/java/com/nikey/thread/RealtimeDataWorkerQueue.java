package com.nikey.thread;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @auther: ccy
 * @date: 2019/07/31 09:59
 * @description:
 */
public class RealtimeDataWorkerQueue {

    private static RealtimeDataWorkerQueue instance = new RealtimeDataWorkerQueue();

    public static RealtimeDataWorkerQueue instance() { return instance; }

    private RealtimeDataWorkerQueue() {}

    private final ConcurrentLinkedQueue<Map<String, String[]>> queue = new ConcurrentLinkedQueue<Map<String, String[]>>();

    // lock & condition
    private final Lock lock = new ReentrantLock();
    private final Condition monitor = lock.newCondition();

    /**
     * put data to queue
     */
    public void put(Map<String, String[]> value) {
        try {
            lock.lock();
            //TODO 没有加入队列
            queue.add(value);
            monitor.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * get data from queue
     *
     * @return Map<String, String [ ]>
     */
    public Map<String, String[]> get() throws InterruptedException {
        Map<String, String[]> value = null;
        try {
            lock.lock();
            value = queue.poll();
            if (value == null) {
                monitor.await();
            }
        } finally {
            lock.unlock();
        }
        // TODO return 不为NULL队列仅有一个元素也再次poll？
//		return queue.poll();
        return value;
    }
}

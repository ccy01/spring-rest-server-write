package com.nikey.thread;

import com.nikey.bean.HokoPointinfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * @auther: ccy
 * @date: 2019/07/18 02:26
 * @description: 将hoko bean加入该队列进行消费
 */
public class HokoemcWorkerQueue {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private volatile int complete = 0;//记录消费队列的数量，是唤醒于阻塞的标志。

    private static HokoemcWorkerQueue instance = new HokoemcWorkerQueue();

    private final ConcurrentLinkedQueue<HokoPointinfo> queue = new ConcurrentLinkedQueue<>();

    public static HokoemcWorkerQueue instance() { return instance;}

    private final Lock lock = new ReentrantLock();
    private final Lock another = new ReentrantLock();//另一个锁用于锁住 HokoemcPostThread这个线程
    private final Condition monitor = lock.newCondition();
    private final Condition anotherMonitor = another.newCondition();


    private HokoemcWorkerQueue(){}
    /**
     *
     * @param value
     */

    public void put(HokoPointinfo value) {
        try {
            lock.lock();
            queue.add(value);
            monitor.signal();//唤醒
            logger.info("唤醒某个线程");
        } finally {
            lock.unlock();
        }
    }

    /**
     *
     * @return HokoPointInfo
     * @throws InterruptedException
     */
    public HokoPointinfo get() throws InterruptedException {

        HokoPointinfo value = null;
        try {
            lock.lock();
            value = queue.poll();//某个线程得不到bean就会放入等待队列。
            if(value == null ) {
                logger.info("锁住某个线程");
                monitor.await();
            }
        } finally {
            lock.unlock();
        }
        return value;
    }

    public boolean isComplete() {
        return complete == 0;
    }

    public void increment(){
        synchronized (this){
            complete++;
        }
        logger.info("等待消费的数量是：" + complete);
    }
    public void decrease(){
        synchronized (this){
            complete--;
        }
        logger.info("等待消费的数量是：" + complete);
    }

    public void await() throws InterruptedException {//加入等待队列
        try {
            another.lock();
            anotherMonitor.await();
        }finally {
            another.unlock();
        }

    }
    public void signal(){//唤醒等待线程
        if(complete == 0) {
            try {
                another.lock();
                anotherMonitor.signal();
            } finally {
                another.unlock();
            }
        }
    }

}

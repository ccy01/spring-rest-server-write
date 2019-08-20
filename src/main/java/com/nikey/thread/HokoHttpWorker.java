package com.nikey.thread;

import com.nikey.bean.HokoPointinfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;

/**
 * @auther: ccy
 * @date: 2019/07/18 02:51
 * @description: 工作线程负责http请求
 */
public class HokoHttpWorker implements Runnable {

    Logger logger = LoggerFactory.getLogger(getClass());

    private final ExecutorService executor;
    private Work callable;

    public HokoHttpWorker(ExecutorService executor, Work callable) {
        this.executor = executor;
        this.callable = callable;
    }

    @Override
    public void run() {
        work();
    }

    private void work() {

        while (!Thread.currentThread().isInterrupted() && !WorkQueue.instance().getStopWorking()) {
            logger.info("HokoHttpWorker thread start work  now-----------------------------");
            try {
                HokoPointinfo value = HokoemcWorkerQueue.instance().get();
                if (value != null) {
                    try {
                        callable.call(value);
                    } catch (Exception e) {
                        logger.warn("HokoemcPostThread的work方法发生未知错误");
                        e.printStackTrace();
                    } finally {
                        HokoemcWorkerQueue.instance().decrease();//标志位减少
                        HokoemcWorkerQueue.instance().signal();//如果为0唤醒线程
                    }
                }

            } catch (InterruptedException e) {
                if (!executor.isShutdown()) {
                    // interrupt
                    logger.warn("Thread {} has been interrupted.", Thread.currentThread().getName());
                    // end the thread's work
                    return;
                }
            }
        }

    }
}

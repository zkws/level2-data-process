package com.alphainv.tdfapi;

import java.util.concurrent.*;

/**
 * <p>
 * [任务调度系统]
 * <br>
 * [后台守护线程不断的执行检测工作]
 * </p>
 *
 * @author wangguangdong
 * @version 1.0
 * @Date 2015年11月23日14:19:40
 */
public class TaskQueueDaemonThread {

    private TaskQueueDaemonThread() {
    }

    private static class LazyHolder {
        private static TaskQueueDaemonThread taskQueueDaemonThread = new TaskQueueDaemonThread();
    }

    public static TaskQueueDaemonThread getInstance() {
        return LazyHolder.taskQueueDaemonThread;
    }

    int poolSize = 200;
//    BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(600000);
//    RejectedExecutionHandler policy = new ThreadPoolExecutor.DiscardPolicy();
//    ExecutorService executorService = new ThreadPoolExecutor(poolSize, Integer.MAX_VALUE,0, TimeUnit.SECONDS, queue, policy);
    Executor executor = Executors.newFixedThreadPool(poolSize);
    /**
     * 守护线程
     */
    private Thread daemonThread;

    /**
     * 初始化守护线程
     */
    public void init() {
        daemonThread = new Thread(() -> execute());
        daemonThread.setDaemon(true);
        daemonThread.setName("Task Queue Daemon Thread");
        daemonThread.start();
    }

    private void execute() {
//        System.out.println("start:" + System.currentTimeMillis());
        while (true) {
            try {
                //从延迟队列中取值,如果没有对象过期则队列一直等待，
                Task t1 = t.take();
                if (t1 != null) {
                    //修改问题的状态
                    Runnable task = t1.getTask();
                    if (task == null) {
                        continue;
                    }
//                    executorService.execute(task);
                    executor.execute(task);
//                    System.out.println("[at task:" + task + "]   [Time:" + System.currentTimeMillis() + "]");
                }
            } catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    /**
     * 创建一个最初为空的新 DelayQueue
     */
    private DelayQueue<Task> t = new DelayQueue<>();

    /**
     * 添加任务，
     * time 延迟时间
     * task 任务
     * 用户为问题设置延迟时间
     */
    public void put(long time, Runnable task) {
        //转换成ns
        long nanoTime = TimeUnit.NANOSECONDS.convert(time, TimeUnit.MILLISECONDS);
        //创建一个任务
        Task k = new Task(nanoTime, task);
        //将任务放在延迟的队列中
        t.put(k);
    }

    /**
     * 结束订单
     * @param task
     */
    public boolean endTask(Task<Runnable> task){
        return t.remove(task);
    }

}


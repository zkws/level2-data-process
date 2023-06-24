package com.alphainv.tdfapi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

public class ChannelStock5MBuyOrderStat implements Runnable{
    protected Boolean quitFlag = false;

    private DelayQueue<DelayedOrder> m5OrderBuyDelayQueue = new DelayQueue<DelayedOrder>();

    private ConcurrentHashMap<String,Long> order5MBuyMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStock5MBuyOrderStat(DelayQueue<DelayedOrder> m5OrderBuyDelayQueue, ConcurrentHashMap<String,Long> order5MBuyMap){
        this.m5OrderBuyDelayQueue=m5OrderBuyDelayQueue;
        this.order5MBuyMap=order5MBuyMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            try {
                DelayedOrder delayedOrderInstance = m5OrderBuyDelayQueue.take();
                Long new5MOrderTotalValue = 0L;
                if (delayedOrderInstance!=null){
                    String stkCode = delayedOrderInstance.getStkCode();
                    Long turnoverValue = delayedOrderInstance.getOrderTurnover();
                    synchronized(order5MBuyMap){
                        Long order5MTotalValue = order5MBuyMap.get(stkCode);
                        order5MTotalValue = order5MTotalValue!= null?order5MTotalValue:0;
                        new5MOrderTotalValue = order5MTotalValue-turnoverValue;
                        order5MBuyMap.put(stkCode,new5MOrderTotalValue);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}

package com.alphainv.tdfapi;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

public class ChannelStock5MSellOrderStat implements Runnable{
    protected Boolean quitFlag = false;

    private DelayQueue<DelayedOrder> m5OrderSellDelayQueue = new DelayQueue<DelayedOrder>();

    private ConcurrentHashMap<String,Long> order5MSellMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStock5MSellOrderStat(DelayQueue<DelayedOrder> m5OrderSellDelayQueue, ConcurrentHashMap<String,Long> order5MSellMap){
        this.m5OrderSellDelayQueue=m5OrderSellDelayQueue;
        this.order5MSellMap=order5MSellMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            try {
                DelayedOrder delayedOrderInstance = m5OrderSellDelayQueue.take();
                Long new5MOrderTotalValue = 0L;
                if (delayedOrderInstance!=null){
                    String stkCode = delayedOrderInstance.getStkCode();
                    Long turnoverValue = delayedOrderInstance.getOrderTurnover();
                    synchronized(order5MSellMap){
                        Long order5MTotalValue = order5MSellMap.get(stkCode);
                        order5MTotalValue = order5MTotalValue!= null?order5MTotalValue:0;
                        new5MOrderTotalValue = order5MTotalValue-turnoverValue;
                        order5MSellMap.put(stkCode,new5MOrderTotalValue);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}

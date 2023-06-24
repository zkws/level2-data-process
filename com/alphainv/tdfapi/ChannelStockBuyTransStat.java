package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelStockBuyTransStat implements Runnable{
    protected Boolean quitFlag = false;

    private BlockingQueue<Object[]> transBuyQueue = new LinkedBlockingQueue<Object[]>();

    private ConcurrentHashMap<String,Long> transBuyMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStockBuyTransStat(BlockingQueue<Object[]> transBuyQueue, ConcurrentHashMap<String,Long> transBuyMap){
        this.transBuyQueue=transBuyQueue;
        this.transBuyMap=transBuyMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            Object[] buyTransArray = transBuyQueue.poll();
            if (buyTransArray!=null){
                String stkCode = (String) buyTransArray[0];
                Long turnoverValue = (Long)buyTransArray[1];
                Long transTotalValue = transBuyMap.get(stkCode);
                transTotalValue = transTotalValue!= null?transTotalValue:0;
                long newTransTotalValue = transTotalValue+turnoverValue;
                transBuyMap.put(stkCode,newTransTotalValue);
            }
        }
    }
}

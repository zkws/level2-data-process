package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class ChannelStockSellTransStat implements Runnable{
    protected Boolean quitFlag = false;

    private BlockingQueue<Object[]> transSellQueue = new LinkedBlockingQueue<Object[]>();

    private ConcurrentHashMap<String,Long> transSellMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStockSellTransStat(BlockingQueue<Object[]> transSellQueue, ConcurrentHashMap<String,Long> transSellMap){
        this.transSellQueue=transSellQueue;
        this.transSellMap=transSellMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            Object[] SellTransArray = transSellQueue.poll();
            if (SellTransArray!=null){
                String stkCode = (String) SellTransArray[0];
                Long turnoverValue = (Long)SellTransArray[1];
                Long transTotalValue = transSellMap.get(stkCode);
                transTotalValue = transTotalValue!= null?transTotalValue:0;
                long newTransTotalValue = transTotalValue+turnoverValue;
                transSellMap.put(stkCode,newTransTotalValue);
            }
        }
    }
}

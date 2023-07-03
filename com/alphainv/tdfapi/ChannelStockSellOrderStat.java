package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelStockSellOrderStat implements Runnable{
    protected Boolean quitFlag = false;

    private BlockingQueue<Object[]> orderSellQueue;

    private ConcurrentHashMap<String,Long> newestPriceMap;

    private ConcurrentHashMap<String,Integer> highLimitFlagMap;

    private ConcurrentHashMap<String,Long> orderSellMap;

    private ConcurrentHashMap<String,Double> orderSellWeightedMap;

    private ConcurrentHashMap<String,Long> order5MSellMap;

    private ConcurrentHashMap<String,Long> orderSellTrueMap;

    private ConcurrentHashMap<String,Double> orderSellWeightedTrueMap;

    private ConcurrentHashMap<String,Long> order5MSellTrueMap;


    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    public ChannelStockSellOrderStat(BlockingQueue<Object[]> orderSellQueue, ConcurrentHashMap<String, Long> newestPriceMap, ConcurrentHashMap<String, Integer> highLimitFlagMap,
                                     ConcurrentHashMap<String, Long> orderSellMap, ConcurrentHashMap<String, Double> orderSellWeightedMap, ConcurrentHashMap<String, Long> order5MSellMap,
                                     ConcurrentHashMap<String, Long> orderSellTrueMap, ConcurrentHashMap<String, Double> orderSellWeightedTrueMap, ConcurrentHashMap<String, Long> order5MSellTrueMap) {
        this.orderSellQueue = orderSellQueue;
        this.newestPriceMap = newestPriceMap;
        this.highLimitFlagMap = highLimitFlagMap;
        this.orderSellMap = orderSellMap;
        this.orderSellWeightedMap = orderSellWeightedMap;
        this.order5MSellMap = order5MSellMap;
        this.orderSellTrueMap = orderSellTrueMap;
        this.orderSellWeightedTrueMap = orderSellWeightedTrueMap;
        this.order5MSellTrueMap = order5MSellTrueMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            Object[] sellOrderArray = orderSellQueue.poll();
            if (sellOrderArray!=null){
                Long highLimitOrderNum = 10000L;
                String stkCode = (String) sellOrderArray[0];
                Long turnoverValue = (Long)sellOrderArray[1];
                Double weightedTurnoverValue = (Double)sellOrderArray[2];
                Integer highLimitFlag = highLimitFlagMap.get(stkCode);
                Long newestPrice = newestPriceMap.get(stkCode);
                newestPrice = newestPrice!= null?newestPrice:0;
                Long newOrderTotalValue = 0L;
                Long new5MOrderTotalValue = 0L;
                Double newWeightedOrderTotalValue = 0D;
                Long orderTotalValue = 0L;
                Double weightedOrderTotalValue = 0D;
                long highLimitTurnover = newestPrice*highLimitOrderNum;
                synchronized(orderSellMap){
                    orderTotalValue = orderSellTrueMap.get(stkCode);
                    orderTotalValue = orderTotalValue!= null?orderTotalValue:0;
                    newOrderTotalValue = orderTotalValue+turnoverValue;
                    orderSellTrueMap.put(stkCode,newOrderTotalValue);
                    if (null!=highLimitFlag&&highLimitFlag.equals(1)){
//                    long highLimitTurnover = newestPrice*highLimitOrderNum/10000;
                        if (newOrderTotalValue<highLimitTurnover){
                            newOrderTotalValue = highLimitTurnover;
                        }
                    }
                    orderSellMap.put(stkCode,newOrderTotalValue);
                }

                synchronized(orderSellWeightedMap){
                    weightedOrderTotalValue = orderSellWeightedTrueMap.get(stkCode);
                    weightedOrderTotalValue = weightedOrderTotalValue!= null?weightedOrderTotalValue:0;
                    newWeightedOrderTotalValue = weightedOrderTotalValue+weightedTurnoverValue;
                    orderSellWeightedTrueMap.put(stkCode,newWeightedOrderTotalValue);
                    if (null!=highLimitFlag&&highLimitFlag.equals(1)){
//                    long lowLimitTurnover = newestPrice*lowLimitOrderNum/10000;
                        if (newWeightedOrderTotalValue<highLimitTurnover){
                            newWeightedOrderTotalValue = (double)highLimitTurnover;
                        }
                    }
                    orderSellWeightedMap.put(stkCode,newWeightedOrderTotalValue);
                }

                synchronized(order5MSellMap){
                    Long order5MTotalValue = order5MSellTrueMap.get(stkCode);
                    order5MTotalValue = order5MTotalValue!= null?order5MTotalValue:0;
                    new5MOrderTotalValue = order5MTotalValue+turnoverValue;
                    order5MSellTrueMap.put(stkCode,new5MOrderTotalValue);
                    if (null!=highLimitFlag&&highLimitFlag.equals(1)){
//                    long highLimitTurnover = newestPrice*highLimitOrderNum/10000;
                        if (new5MOrderTotalValue < highLimitTurnover){
                            new5MOrderTotalValue = highLimitTurnover;
                        }
                    }
                    order5MSellMap.put(stkCode,new5MOrderTotalValue);
                }
            }
        }
    }
}

package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelStockSellOrderStat implements Runnable{
    protected Boolean quitFlag = false;

    private BlockingQueue<Object[]> orderSellQueue;

    private ConcurrentHashMap<String,Long> newestPriceMap;

    private ConcurrentHashMap<String,Integer> highLimitFlagMap;

    private ConcurrentHashMap<String,Long> orderSellMap;

    private ConcurrentHashMap<String,Long> order5MSellMap;

    private ConcurrentHashMap<String,Double> orderSellWeightedMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStockSellOrderStat(BlockingQueue<Object[]> orderSellQueue,
                              ConcurrentHashMap<String,Long> newestPriceMap, ConcurrentHashMap<String,Integer> highLimitFlagMap,
                              ConcurrentHashMap<String,Long> orderSellMap, ConcurrentHashMap<String,Double> orderSellWeightedMap, ConcurrentHashMap<String,Long> order5MSellMap){
        this.orderSellQueue=orderSellQueue;
        this.newestPriceMap=newestPriceMap;
        this.highLimitFlagMap=highLimitFlagMap;
        this.orderSellMap=orderSellMap;
        this.orderSellWeightedMap=orderSellWeightedMap;
        this.order5MSellMap=order5MSellMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            Object[] sellOrderArray = orderSellQueue.poll();
            if (sellOrderArray!=null){
                Long highLimitOrderNum = 10000L;
                String stkCode = (String) sellOrderArray[0];
                Long turnoverValue = (Long)sellOrderArray[1];
                Long orderPrice = (Long)sellOrderArray[2];
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
                    orderTotalValue = orderSellMap.get(stkCode);
                    orderTotalValue = orderTotalValue!= null?orderTotalValue:0;
                    newOrderTotalValue = orderTotalValue+turnoverValue;
                    if (null!=highLimitFlag&&highLimitFlag.equals(1)){
//                    long highLimitTurnover = newestPrice*highLimitOrderNum/10000;
                        if (newOrderTotalValue<highLimitTurnover){
                            newOrderTotalValue = highLimitTurnover;
                        }
                    }
                    orderSellMap.put(stkCode,newOrderTotalValue);
                }
                double orderPriceWeight = WeightCalUtil.getOrderPriceWeight(newestPrice,orderPrice);
                Double weightedTurnoverValue = orderPriceWeight*turnoverValue;

                synchronized(orderSellWeightedMap){
                    weightedOrderTotalValue = orderSellWeightedMap.get(stkCode);
                    weightedOrderTotalValue = weightedOrderTotalValue!= null?weightedOrderTotalValue:0;
                    newWeightedOrderTotalValue = weightedOrderTotalValue+weightedTurnoverValue;
                    if (null!=highLimitFlag&&highLimitFlag.equals(1)){
//                    long lowLimitTurnover = newestPrice*lowLimitOrderNum/10000;
                        if (newWeightedOrderTotalValue<highLimitTurnover){
                            newWeightedOrderTotalValue = (double)highLimitTurnover;
                        }
                    }
                    orderSellWeightedMap.put(stkCode,newWeightedOrderTotalValue);
                }

                synchronized(order5MSellMap){
                    Long order5MTotalValue = order5MSellMap.get(stkCode);
                    order5MTotalValue = order5MTotalValue!= null?order5MTotalValue:0;
                    new5MOrderTotalValue = order5MTotalValue+turnoverValue;
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

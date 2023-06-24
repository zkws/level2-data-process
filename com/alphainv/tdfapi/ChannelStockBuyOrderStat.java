package com.alphainv.tdfapi;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ChannelStockBuyOrderStat implements Runnable{
    protected Boolean quitFlag = false;

    private BlockingQueue<Object[]> orderBuyQueue;

    private ConcurrentHashMap<String,Long> newestPriceMap;

    private ConcurrentHashMap<String,Integer> lowLimitFlagMap;

    private ConcurrentHashMap<String,Long> orderBuyMap;

    private ConcurrentHashMap<String,Double> orderBuyWeightedMap;

    private ConcurrentHashMap<String,Long> order5MBuyMap;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;
    }

    ChannelStockBuyOrderStat(BlockingQueue<Object[]> orderBuyQueue,
                          ConcurrentHashMap<String,Long> newestPriceMap,ConcurrentHashMap<String,Integer> lowLimitFlagMap,
                          ConcurrentHashMap<String,Long> orderBuyMap,ConcurrentHashMap<String,Double> orderBuyWeightedMap,ConcurrentHashMap<String,Long> order5MBuyMap){
        this.orderBuyQueue=orderBuyQueue;
        this.newestPriceMap=newestPriceMap;
        this.lowLimitFlagMap=lowLimitFlagMap;
        this.orderBuyMap=orderBuyMap;
        this.orderBuyWeightedMap=orderBuyWeightedMap;
        this.order5MBuyMap=order5MBuyMap;
    }

    @Override
    public void run() {
        while (!quitFlag){
            Object[] buyOrderArray = orderBuyQueue.poll();
            if (buyOrderArray!=null){
                Long lowLimitOrderNum = 10000L;
                String stkCode = (String) buyOrderArray[0];
                Long turnoverValue = (Long)buyOrderArray[1];
                Long orderPrice = (Long)buyOrderArray[2];
                Integer lowLimitFlag = lowLimitFlagMap.get(stkCode);
                Long newestPrice =newestPriceMap.get(stkCode);
                newestPrice = newestPrice!= null?newestPrice:0;
                Long newOrderTotalValue = 0L;
                Long new5MOrderTotalValue = 0L;
                Double newWeightedOrderTotalValue = 0D;
                Long orderTotalValue = 0L;
                Double weightedOrderTotalValue = 0D;
                long lowLimitTurnover = newestPrice*lowLimitOrderNum;
                synchronized(orderBuyMap){
                    orderTotalValue = orderBuyMap.get(stkCode);
                    orderTotalValue = orderTotalValue!= null?orderTotalValue:0;
                    newOrderTotalValue = orderTotalValue+turnoverValue;
                    if (null!=lowLimitFlag&&lowLimitFlag.equals(1)){
//                    long lowLimitTurnover = newestPrice*lowLimitOrderNum/10000;
                        if (newOrderTotalValue<lowLimitTurnover){
                            newOrderTotalValue = lowLimitTurnover;
                        }
                    }
                    orderBuyMap.put(stkCode,newOrderTotalValue);
                }
                double orderPriceWeight = WeightCalUtil.getOrderPriceWeight(newestPrice,orderPrice);
                Double weightedTurnoverValue = orderPriceWeight*turnoverValue;

                synchronized(orderBuyWeightedMap){
                    weightedOrderTotalValue = orderBuyWeightedMap.get(stkCode);
                    weightedOrderTotalValue = weightedOrderTotalValue!= null?weightedOrderTotalValue:0;
                    newWeightedOrderTotalValue = weightedOrderTotalValue+weightedTurnoverValue;
                    if (null!=lowLimitFlag&&lowLimitFlag.equals(1)){
//                    long lowLimitTurnover = newestPrice*lowLimitOrderNum/10000;
                        if (newWeightedOrderTotalValue<lowLimitTurnover){
                            newWeightedOrderTotalValue = (double)lowLimitTurnover;
                        }
                    }
                    orderBuyWeightedMap.put(stkCode,newWeightedOrderTotalValue);
                }
                synchronized(order5MBuyMap){
                    Long order5MTotalValue = order5MBuyMap.get(stkCode);
                    order5MTotalValue = order5MTotalValue!= null?order5MTotalValue:0;
                    new5MOrderTotalValue = order5MTotalValue+turnoverValue;
                    if (null!=lowLimitFlag&&lowLimitFlag.equals(1)){
//                    long lowLimitTurnover = newestPrice*lowLimitOrderNum/10000;
                        if (new5MOrderTotalValue<lowLimitTurnover){
                            new5MOrderTotalValue = lowLimitTurnover;
                        }
                    }
                    order5MBuyMap.put(stkCode,new5MOrderTotalValue);
                }
            }
        }
    }
}

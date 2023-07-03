package com.alphainv.tdfapi;

import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.DelayQueue;

public class OrderDataInfoWrite implements Runnable{
    protected Boolean quitFlag = false;
    BlockingQueue<OrderDataInfo> orderDataInfoQueue;
    ChannelQueueData[] channelQueueDataArray;
    HashMap<String,Integer> channelCodeMap;
    HashMap<String,String> stkChannelMap;

    OrderDataInfoWrite(ChannelQueueData[] channelQueueDataArray, BlockingQueue<OrderDataInfo> orderDataInfoQueue, HashMap<String,Integer> channelCodeMap, HashMap<String,String> stkChannelMap){
        this.channelQueueDataArray = channelQueueDataArray;
        this.orderDataInfoQueue = orderDataInfoQueue;
        this.channelCodeMap = channelCodeMap;
        this.stkChannelMap = stkChannelMap;
    }

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }

    @Override
    public void run() {
        long countNum = 0l;
//        long turnOver0Count = 0l;
        while (!quitFlag){
            OrderDataInfo orderDataInfoInstance = orderDataInfoQueue.poll();
            if (countNum%200000==0){
                int queueSize = orderDataInfoQueue.size();
                System.out.println("已处理"+countNum+", 当前order队列长度还剩 "+queueSize);
            }
//            if (turnOver0Count%1000==0){
//                System.out.println("turnOver为0的共计"+turnOver0Count);
//            }
            if (orderDataInfoInstance!=null){
                String singleStockCodeNum = orderDataInfoInstance.getStockCode();
                if (singleStockCodeNum.startsWith("6")||singleStockCodeNum.startsWith("0")||singleStockCodeNum.startsWith("3")){
                    String exchangeTimeStr = orderDataInfoInstance.getExchangeTime();
                    char stkFunctionCode = orderDataInfoInstance.getStkFunctionCode();
                    char stkOrderKind = orderDataInfoInstance.getStkOrderKind();
                    int orderVolume = orderDataInfoInstance.getOrderVolume();
                    int orderCode = orderDataInfoInstance.getOrderCode();
                    int channelCode = orderDataInfoInstance.getChannelId();
                    long orderPrice = orderDataInfoInstance.getOrderPrice();
                    Long orderOriNo = orderDataInfoInstance.getOrderOriNo();
                    int channelIndex = channelCodeMap.get(String.valueOf(channelCode));
                    stkChannelMap.put(singleStockCodeNum,String.valueOf(channelCode));
                    ChannelQueueData channelQueueData = channelQueueDataArray[channelIndex];
                    BlockingQueue<Object[]> orderBuyQueue = channelQueueData.getOrderBuyQueue();
                    BlockingQueue<Object[]> orderSellQueue = channelQueueData.getOrderSellQueue();
                    DelayQueue<DelayedOrder> m5OrderBuyDelayQueue = channelQueueData.getM5OrderBuyDelayQueue();
                    DelayQueue<DelayedOrder> m5OrderSellDelayQueue = channelQueueData.getM5OrderSellDelayQueue();
                    ConcurrentHashMap<String,Long> newestPriceMap = channelQueueData.getNewestPriceMap();
                    ConcurrentHashMap<String,Long> highLimitPriceMap = channelQueueData.getHighLimitPriceMap();
                    ConcurrentHashMap<String,Long> lowLimitPriceMap = channelQueueData.getLowLimitPriceMap();
                    Object[] orderInfoArray = new Object[3];
                    orderInfoArray[0] = singleStockCodeNum;
                    Long newestPrice = newestPriceMap.get(singleStockCodeNum);
                    newestPrice = newestPrice != null?newestPrice:0L;
                    Long[] orderPriceArray = channelQueueData.getOrderPriceArray();
                    Double[] orderSHPriceWeightArray = channelQueueData.getOrderSHPriceWeightArray();
                    Double[] orderSZPriceWeightArray = channelQueueData.getOrderSZPriceWeightArray();
                    if (orderPrice== 0L){
//                    System.out.println("StockCode:"+singleStockCodeNum+" orderPrice:"+orderPrice+" orderVolume:"+orderVolume+" 方向："+stkFunctionCode+" 时间："+exchangeTimeStr);
//                    System.out.println("StockCode:"+singleStockCodeNum+" 最新价:"+newestPriceMap.get(singleStockCodeNum)+" 涨停价:"+highLimitPriceMap.get(singleStockCodeNum)+" 跌停价："+lowLimitPriceMap.get(singleStockCodeNum));
//                    turnOver0Count++;
                        orderPrice = newestPrice;
                    }
                    long orderTurnover = orderPrice*orderVolume;
                    orderPriceArray[orderCode]=orderPrice;
                    Double orderPriceWeight = 0D;

                    if (stkOrderKind == 'D'){
                        orderTurnover = -1*orderTurnover;
                        orderPriceWeight = orderSHPriceWeightArray[orderOriNo.intValue()];
                        if (orderPriceWeight==null){
                            orderDataInfoQueue.offer(orderDataInfoInstance);
                            continue;
                        }
                    }
                    else {
                        orderPriceWeight = WeightCalUtil.getOrderPriceWeight(newestPrice,orderPrice);
                    }
                    if (singleStockCodeNum.startsWith("6")){
                        //上交所撤单数据可以根据orderOriNo与原订单数据对应
                        orderSHPriceWeightArray[orderOriNo.intValue()]=orderPriceWeight;
                    }
                    else{
                        orderSZPriceWeightArray[orderCode]=orderPriceWeight;
                    }
                    Double weightedTurnoverValue = orderPriceWeight*orderTurnover;
                    DelayedOrder delayedOrder = new DelayedOrder(singleStockCodeNum,orderTurnover,300000);
                    orderInfoArray[1] = orderTurnover;
                    orderInfoArray[2] = weightedTurnoverValue;
                    if (stkFunctionCode == 'B'){
                        try {
                            orderBuyQueue.put(orderInfoArray);
                            m5OrderBuyDelayQueue.put(delayedOrder);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    else if (stkFunctionCode == 'S'){
                        try {
                            orderSellQueue.put(orderInfoArray);
                            m5OrderSellDelayQueue.put(delayedOrder);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    countNum++;
                }
            }
        }
    }
}

package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;

public class AllStockStat implements Runnable{
    HashMap<String,String> stkChannelMap;

    ChannelQueueData[] channelQueueDataArray;

    HashMap<String,Integer> channelCodeMap;

    JedisPool jedisPool;

    String[] channelArray;

    ConcurrentHashMap<String,String> stkSDMapAll;
    ConcurrentHashMap<String,Long> orderBuyMapAll;
    ConcurrentHashMap<String,Long> orderSellMapAll;
    ConcurrentHashMap<String,Double> weightedOrderBuyMapAll;
    ConcurrentHashMap<String,Double> weightedOrderSellMapAll;
    ConcurrentHashMap<String,Long> order5MBuyMapAll;
    ConcurrentHashMap<String,Long> order5MSellMapAll;
    ConcurrentHashMap<String,Long> transBuyMapAll;
    ConcurrentHashMap<String,Long> transSellMapAll;
    ConcurrentHashMap<String,Double> orderBsRateMapALL;
    ConcurrentHashMap<String,Double> weightedOrderBSRateMapAll;
    ConcurrentHashMap<String,Double> transBsRateMapALL;
    ConcurrentHashMap<String, ArrayList<Double>> compositeScoreMapALL;
    ConcurrentHashMap<String,Double> indexSDMapAll;

    public AllStockStat(HashMap<String, String> stkChannelMap, ChannelQueueData[] channelQueueDataArray,
                        HashMap<String, Integer> channelCodeMap, JedisPool jedisPool, String[] channelArray,
                        ConcurrentHashMap<String, String> stkSDMapAll,
                        ConcurrentHashMap<String, Long> orderBuyMapAll, ConcurrentHashMap<String, Long> orderSellMapAll,
                        ConcurrentHashMap<String,Double> weightedOrderBuyMapAll, ConcurrentHashMap<String,Double> weightedOrderSellMapAll,
                        ConcurrentHashMap<String, Long> order5MBuyMapAll, ConcurrentHashMap<String, Long> order5MSellMapAll,
                        ConcurrentHashMap<String, Long> transBuyMapAll, ConcurrentHashMap<String, Long> transSellMapAll,
                        ConcurrentHashMap<String,Double> orderBsRateMapALL,
                        ConcurrentHashMap<String,Double> weightedOrderBSRateMapAll,
                        ConcurrentHashMap<String,Double> transBsRateMapALL,
                        ConcurrentHashMap<String, ArrayList<Double>> compositeScoreMapALL,
                        ConcurrentHashMap<String,Double> indexSDMapAll
                        ) {
        this.stkChannelMap = stkChannelMap;
        this.channelQueueDataArray = channelQueueDataArray;
        this.channelCodeMap = channelCodeMap;
        this.jedisPool = jedisPool;
        this.channelArray = channelArray;
        this.stkSDMapAll = stkSDMapAll;
        this.orderBuyMapAll = orderBuyMapAll;
        this.orderSellMapAll = orderSellMapAll;
        this.weightedOrderBuyMapAll = weightedOrderBuyMapAll;
        this.weightedOrderSellMapAll = weightedOrderSellMapAll;
        this.order5MBuyMapAll = order5MBuyMapAll;
        this.order5MSellMapAll = order5MSellMapAll;
        this.transBuyMapAll = transBuyMapAll;
        this.transSellMapAll = transSellMapAll;
        this.orderBsRateMapALL = orderBsRateMapALL;
        this.weightedOrderBSRateMapAll = weightedOrderBSRateMapAll;
        this.transBsRateMapALL = transBsRateMapALL;
        this.compositeScoreMapALL = compositeScoreMapALL;
        this.indexSDMapAll = indexSDMapAll;
    }

    @Override
    public void run() {
        Date statDate = new Date();
        long statDateLong = statDate.getTime();
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        String ymdStr = ymdDF.format(statDate);
        Jedis realJedisInstance = null;
        try {
            HashSet<String> availableStkSet = DataBaseOperation.getAvailableStkSet();
            realJedisInstance = jedisPool.getResource();
            for (int i=0;i<channelArray.length;i++){
                ChannelQueueData channelQueueDataInstance = channelQueueDataArray[i];
                ConcurrentHashMap<String,String> stkSDMap = channelQueueDataInstance.getStkSDMap();;
                ConcurrentHashMap<String,Long> orderBuyMap = channelQueueDataInstance.getOrderBuyMap();
                ConcurrentHashMap<String,Long> orderSellMap =channelQueueDataInstance.getOrderSellMap();
                ConcurrentHashMap<String,Double> orderBuyWeightedMap = channelQueueDataInstance.getOrderBuyWeightedMap();
                ConcurrentHashMap<String,Double> orderSellWeightedMap = channelQueueDataInstance.getOrderSellWeightedMap();
                ConcurrentHashMap<String,Long> order5MBuyMap = channelQueueDataInstance.getOrder5MBuyMap();
                ConcurrentHashMap<String,Long> order5MSellMap = channelQueueDataInstance.getOrder5MSellMap();
                ConcurrentHashMap<String,Long> transBuyMap = channelQueueDataInstance.getTransBuyMap();
                ConcurrentHashMap<String,Long> transSellMap = channelQueueDataInstance.getTransSellMap();
                for (String stkCode:orderBuyMap.keySet()) {
                    if (availableStkSet.contains(stkCode)){
                        String SDRateKey = ymdStr+"-"+stkCode+"-SDRate";
                        String transBSRateKey = ymdStr+"-"+stkCode+"-TBSRATE";
                        String orderBSRateKey = ymdStr+"-"+stkCode+"-OBSRATE";
                        String weightedOrderBSRateKey = ymdStr+"-"+stkCode+"-WOBSRATE";
                        String orderBSRateSourceKey = ymdStr+"-"+stkCode+"-OBSRATES";
                        String orderBSRateChangeRateKey = ymdStr+"-"+stkCode+"-OBSRATECR";
                        String orderBSRateM5Key = ymdStr+"-"+stkCode+"-OBSRATEM5";
                        String orderBSRateM5SourceKey = ymdStr+"-"+stkCode+"-OBSRATEM5S";
                        String compositeScoreKey = ymdStr+"-"+stkCode+"-CSCORE";
                        String bounceRateKey = ymdStr+"-"+stkCode+"-BOUNCERATE";

                        Long stkTransBuyValue = transBuyMap.get(stkCode);
                        stkTransBuyValue = stkTransBuyValue!= null?stkTransBuyValue:0;
                        transBuyMapAll.put(stkCode,stkTransBuyValue);
                        Long stkTransSellValue = transSellMap.get(stkCode);
                        stkTransSellValue = stkTransSellValue!= null?stkTransSellValue:0;
                        transSellMapAll.put(stkCode,stkTransSellValue);
                        Long stkOrderBuyValue = orderBuyMap.get(stkCode);
                        stkOrderBuyValue = stkOrderBuyValue!= null?stkOrderBuyValue:0;
                        Long lastStkOrderBuyValue = orderBuyMapAll.get(stkCode);
                        orderBuyMapAll.put(stkCode,stkOrderBuyValue);
                        Long stkOrderSellValue = orderSellMap.get(stkCode);
                        stkOrderSellValue = stkOrderSellValue!= null?stkOrderSellValue:0;
                        Long lastStkOrderSellValue = orderSellMapAll.get(stkCode);
                        orderSellMapAll.put(stkCode,stkOrderSellValue);
                        Long stk5mOrderBuyValue = order5MBuyMap.get(stkCode);
                        stk5mOrderBuyValue = stk5mOrderBuyValue!= null?stk5mOrderBuyValue:0;
                        order5MBuyMapAll.put(stkCode,stk5mOrderBuyValue);
                        Long stk5mOrderSellValue = order5MSellMap.get(stkCode);
                        stk5mOrderSellValue = stk5mOrderSellValue!= null?stk5mOrderSellValue:0;
                        order5MSellMapAll.put(stkCode,stk5mOrderSellValue);
                        String stkSDValue = stkSDMap.get(stkCode);
                        stkSDValue = stkSDValue!= null?stkSDValue:"0";
                        stkSDMapAll.put(stkCode,stkSDValue);
                        Double stkSDValueDouble = stkSDValue!= null?Double.parseDouble(stkSDValue):0d;

                        Double stkOrderWeightedBuyValue = orderBuyWeightedMap.get(stkCode);
                        stkOrderWeightedBuyValue = stkOrderWeightedBuyValue!= null?stkOrderWeightedBuyValue:0D;
                        weightedOrderBuyMapAll.put(stkCode,stkOrderWeightedBuyValue);
                        Double stkOrderWeightedSellValue = orderSellWeightedMap.get(stkCode);
                        stkOrderWeightedSellValue = stkOrderWeightedSellValue!= null?stkOrderWeightedSellValue:0D;
                        weightedOrderSellMapAll.put(stkCode,stkOrderWeightedSellValue);

                        realJedisInstance.set(SDRateKey,stkSDValue);

                        String orderBSRate = "0";
                        String orderBSRateSource="";
                        Double orderBSRateValue = 0D;

                        if (stkOrderBuyValue>0){
                            orderBSRateSource+="买正";
                        }
                        else if (stkOrderBuyValue<0){
                            orderBSRateSource+="买负";
                        }
                        else{
                            orderBSRateSource+="买0";
                        }

                        if (stkOrderSellValue>0){
                            orderBSRateSource+="卖正";
                            orderBSRateValue = (double) stkOrderBuyValue /stkOrderSellValue;
                            orderBSRate = String.format("%.1f", orderBSRateValue);
                        }
                        else if (stkOrderSellValue<0){
                            orderBSRateSource+="卖负";
                            orderBSRateValue = (double) stkOrderBuyValue /stkOrderSellValue;
                            orderBSRate = String.format("%.1f", orderBSRateValue);
                        }
                        else{
                            orderBSRateSource+="卖0";
                        }
                        String orderBSRateChangeRate = "0";
                        Double lastOrderBSRate = 0D;
                        if (lastStkOrderBuyValue!=null&&lastStkOrderSellValue!=null&&!lastStkOrderSellValue.equals(0L)){
                            lastOrderBSRate = (double) lastStkOrderBuyValue /lastStkOrderSellValue;
                        }
                        if(!orderBSRateValue.equals(0D)){
                            orderBSRateChangeRate = String.format("%.1f", (orderBSRateValue-lastOrderBSRate)/lastOrderBSRate*100);
                        }
                        orderBsRateMapALL.put(stkCode,orderBSRateValue);
                        realJedisInstance.set(orderBSRateKey,orderBSRate);
                        realJedisInstance.set(orderBSRateSourceKey,orderBSRateSource);

                        String weightedOrderBSRate = "0";
                        Double weightedOrderBSRateValue = 0D;
                        if (!stkOrderWeightedSellValue.equals(0D)){
                            weightedOrderBSRateValue = (double) stkOrderWeightedBuyValue /stkOrderWeightedSellValue;
                            weightedOrderBSRate = String.format("%.1f", weightedOrderBSRateValue);
                        }
                        realJedisInstance.set(weightedOrderBSRateKey,weightedOrderBSRate);
                        weightedOrderBSRateMapAll.put(stkCode, weightedOrderBSRateValue);

//          计算5分钟压托比
                        String orderBSRateM5 = "0";
                        String orderBSRateM5Source="";

                        if (stk5mOrderBuyValue>0){
                            orderBSRateM5Source+="买正";
                        }
                        else if (stk5mOrderBuyValue<0){
                            orderBSRateM5Source+="买负";
                        }
                        else{
                            orderBSRateM5Source+="买0";
                        }

                        if (stk5mOrderSellValue>0){
                            orderBSRateM5Source+="卖正";
                            orderBSRateM5 = String.format("%.1f", (double) stk5mOrderBuyValue /stk5mOrderSellValue);
                        }
                        else if (stk5mOrderSellValue<0){
                            orderBSRateM5Source+="卖负";
                            orderBSRateM5 = String.format("%.1f",(double)stk5mOrderBuyValue/stk5mOrderSellValue);
                        }
                        else{
                            orderBSRateM5Source+="卖0";
                        }
                        realJedisInstance.set(orderBSRateM5Key,orderBSRateM5);
                        realJedisInstance.set(orderBSRateM5SourceKey,orderBSRateM5Source);
                        realJedisInstance.set(orderBSRateChangeRateKey,orderBSRateChangeRate);
//                计算成交主买主卖比
//          TBSRATE: Transaction buy sale rate
                        String transBsRate = "0";
                        Double transBsRateValue = 0D;
                        if (Math.abs(stkTransSellValue)>0){
                            transBsRateValue = (double)stkTransBuyValue/stkTransSellValue;
                            transBsRate = String.format("%.1f",transBsRateValue);
                        }
                        realJedisInstance.set(transBSRateKey,transBsRate);
                        transBsRateMapALL.put(stkCode,transBsRateValue);
//计算综合得分，加权压托比*主买主卖比例
                        Double compositeScoreValue = weightedOrderBSRateValue*transBsRateValue;
                        String compositeScore = String.format("%.1f",compositeScoreValue);
                        realJedisInstance.set(compositeScoreKey,compositeScore);

                        ArrayList<Double> compositeScoreArrayList = new ArrayList<>();
                        compositeScoreArrayList.add(weightedOrderBSRateValue);
                        compositeScoreArrayList.add(transBsRateValue);
                        compositeScoreArrayList.add(compositeScoreValue);
                        compositeScoreMapALL.put(stkCode,compositeScoreArrayList);
//计算弹性比
                        Double indexSDValue = indexSDMapAll.get("300");
                        Double bounceRateValue = 0D;
                        indexSDValue = indexSDValue!= null?indexSDValue:0D;
                        Double bounceValue = stkSDValueDouble-indexSDValue;
                        if (bounceValue>0&&!compositeScoreValue.equals(0D)){
                            bounceRateValue=bounceValue/compositeScoreValue;
                        }
                        else if (bounceValue<0){
                            bounceRateValue=bounceValue*compositeScoreValue;
                        }
                        String bounceRate = String.format("%.1f",bounceRateValue);
                        realJedisInstance.set(bounceRateKey,bounceRate);
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != realJedisInstance) {
                realJedisInstance.close();
            }
        }

        Date finishTime = new Date();
        long finishLong = finishTime.getTime();
        int threadNum = Thread.currentThread().getThreadGroup().activeCount();
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd HHmmss");
        System.out.println(Thread.currentThread().getName()+"已在 "+fullDF.format(finishTime)+" 完成更新所有股票；耗时："+(finishLong-statDateLong)+"毫秒, 当前线程池线程数："+threadNum);
    }
}

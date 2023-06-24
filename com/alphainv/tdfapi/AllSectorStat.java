package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class AllSectorStat implements Runnable{

    JedisPool jedisPool;
    ConcurrentHashMap<String,String> stkSDMapAll;
    ConcurrentHashMap<String,Long> orderBuyMapAll;
    ConcurrentHashMap<String,Long> orderSellMapAll;
    ConcurrentHashMap<String,Double> weightedOrderBuyMapAll;
    ConcurrentHashMap<String,Double> weightedOrderSellMapAll;
    ConcurrentHashMap<String,Long> order5MBuyMapAll;
    ConcurrentHashMap<String,Long> order5MSellMapAll;
    ConcurrentHashMap<String,Long> transBuyMapAll;
    ConcurrentHashMap<String,Long> transSellMapAll;

    public AllSectorStat(JedisPool jedisPool, ConcurrentHashMap<String, String> stkSDMapAll,
                         ConcurrentHashMap<String, Long> orderBuyMapAll, ConcurrentHashMap<String, Long> orderSellMapAll,
                         ConcurrentHashMap<String,Double> weightedOrderBuyMapAll, ConcurrentHashMap<String,Double> weightedOrderSellMapAll,
                         ConcurrentHashMap<String, Long> order5MBuyMapAll, ConcurrentHashMap<String, Long> order5MSellMapAll,
                         ConcurrentHashMap<String, Long> transBuyMapAll, ConcurrentHashMap<String, Long> transSellMapAll) {
        this.jedisPool = jedisPool;
        this.stkSDMapAll = stkSDMapAll;
        this.orderBuyMapAll = orderBuyMapAll;
        this.orderSellMapAll = orderSellMapAll;
        this.weightedOrderBuyMapAll = weightedOrderBuyMapAll;
        this.weightedOrderSellMapAll = weightedOrderSellMapAll;
        this.order5MBuyMapAll = order5MBuyMapAll;
        this.order5MSellMapAll = order5MSellMapAll;
        this.transBuyMapAll = transBuyMapAll;
        this.transSellMapAll = transSellMapAll;
    }

    protected Boolean quitFlag = false;

    public void setQuitFlag(Boolean para) {
        this.quitFlag = para;

    }

    @Override
    public void run() {
        Date statDate = new Date();
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        String ymdStr = ymdDF.format(statDate);
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd HHmmss");
        HashMap<String, ArrayList<String>> swSectorListMap = null;
        HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection = null;
        try {
            swSectorListMap = DataBaseOperation.getSwSectorListMap();
            sectorStkMapCollection = DataBaseOperation.getSectorStkMapCollection(swSectorListMap);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Jedis realJedisInstance = null;
        try {
            // 从连接池获取jedis对象
            realJedisInstance = jedisPool.getResource();
            while (!quitFlag) {
                ArrayList<String> sectorClassList = new ArrayList<String>();
                sectorClassList.add("sw1");
                sectorClassList.add("sw2");
                sectorClassList.add("sw3");
                for (String sectorClass:sectorClassList) {
                    ArrayList<String> sectorCodeList = swSectorListMap.get(sectorClass+"Code");
                    HashMap<String,ArrayList<String>> sectorStkMap = sectorStkMapCollection.get(sectorClass+"Code");
                    for (String sectorCode:sectorCodeList) {
                        ArrayList<String> stkCodeList = sectorStkMap.get(sectorCode);
                        String sectorKeySDRate = ymdStr + "-" +sectorCode+"-SDRate";
                        long sectorTransBuy = 0;
                        long sectorTransSell = 0;
                        long sectorOrderBuy = 0;
                        long sectorOrderSell = 0;
                        Double sectorWeightedOrderBuy = 0D;
                        Double sectorWeightedOrderSell = 0D;
                        long sector5mOrderBuy = 0;
                        long sector5mOrderSell = 0;
                        double sectorTransBuyAvgVal = 0d;
                        double sectorTransSellAvgVal = 0d;
                        double sectorOrderBuyAvgVal = 0d;
                        double sectorOrderSellAvgVal = 0d;
                        Double sectorWeightedOrderBuyAvgVal = 0d;
                        Double sectorWeightedOrderSellAvgVal = 0d;
                        double sector5mOrderBuyAvgVal = 0d;
                        double sector5mOrderSellAvgVal =0d;
                        double sectorSDRate = 0d;
                        String sectorSDRateAvg = "0";
                        int stkCodeListSize = stkCodeList.size();
                        for (int i = 0; i < stkCodeListSize; i++) {
                            String stkCode = stkCodeList.get(i);
                            Long stkTransBuyValue = transBuyMapAll.get(stkCode);
                            Long stkTransSellValue = transSellMapAll.get(stkCode);
                            Long stkOrderBuyValue = orderBuyMapAll.get(stkCode);
                            Long stkOrderSellValue = orderSellMapAll.get(stkCode);
                            Double stkWeightedOrderBuyValue = weightedOrderBuyMapAll.get(stkCode);
                            Double stkWeightedOrderSellValue = weightedOrderSellMapAll.get(stkCode);
                            Long stk5mOrderBuyValue = order5MBuyMapAll.get(stkCode);
                            Long stk5mOrderSellValue = orderSellMapAll.get(stkCode);
                            String stkSDValue = stkSDMapAll.get(stkCode);
                            if (stkTransBuyValue != null) {
                                sectorTransBuy+=stkTransBuyValue;
                            }
                            if (stkTransSellValue != null) {
                                sectorTransSell+=stkTransSellValue;
                            }
                            if (stkOrderBuyValue != null) {
                                sectorOrderBuy+=stkOrderBuyValue;
                            }
                            if (stkOrderSellValue != null) {
                                sectorOrderSell+=stkOrderSellValue;
                            }
                            if (stkWeightedOrderBuyValue != null) {
                                sectorWeightedOrderBuy+=stkWeightedOrderBuyValue;
                            }
                            if (stkWeightedOrderSellValue != null) {
                                sectorWeightedOrderSell+=stkWeightedOrderSellValue;
                            }
                            if (stk5mOrderBuyValue != null) {
                                sector5mOrderBuy+=stk5mOrderBuyValue;
                            }
                            if (stk5mOrderSellValue != null) {
                                sector5mOrderSell+=stk5mOrderSellValue;
                            }
                            if (stkSDValue != null) {
                                sectorSDRate+=Double.parseDouble(stkSDValue);
                            }

                        }
                        if(stkCodeListSize!=0){
                            sectorTransBuyAvgVal = (double)sectorTransBuy/stkCodeListSize;
                            sectorTransSellAvgVal = (double)sectorTransSell/stkCodeListSize;
                            sectorOrderBuyAvgVal = (double)sectorOrderBuy/stkCodeListSize;
                            sectorOrderSellAvgVal = (double)sectorOrderSell/stkCodeListSize;
                            sectorWeightedOrderBuyAvgVal = sectorWeightedOrderBuy/stkCodeListSize;
                            sectorWeightedOrderSellAvgVal = sectorWeightedOrderSell/stkCodeListSize;
                            sector5mOrderBuyAvgVal = (double)sector5mOrderBuy/stkCodeListSize;
                            sector5mOrderSellAvgVal = (double)sector5mOrderSell/stkCodeListSize;
                            sectorSDRateAvg = String.format("%.4f",sectorSDRate/stkCodeListSize);
                        }
//          TBSRATE: Transaction buy sale rate
                        String sectorTransBsRate = "0";
                        String sectorTransBsRateKey =  ymdStr + "-" +sectorCode+"-TBSRATE";
                        if (Math.abs(sectorTransSellAvgVal)>0){
                            sectorTransBsRate = String.format("%.4f",sectorTransBuyAvgVal/sectorTransSellAvgVal);
                        }

                        //          计算压托比
//          OBSRATE: order buy sale rate
//          OBSRATES: order buy sale rate source
                        String sectorOrderBSRate = "0";
                        String sectorOrderBSRateSource="";
                        String sectorOrderBSRateKey = ymdStr + "-" +sectorCode+"-OBSRATE";
                        String sectorOrderBSRateSourceKey = ymdStr + "-" +sectorCode+"-OBSRATES";

                        if (sectorOrderBuyAvgVal>0){
                            sectorOrderBSRateSource+="买正";
                        }
                        else if (sectorOrderBuyAvgVal<0){
                            sectorOrderBSRateSource+="买负";
                        }
                        else{
                            sectorOrderBSRateSource+="买0";
                        }

                        if (sectorOrderSellAvgVal>0){
                            sectorOrderBSRateSource+="卖正";
                            sectorOrderBSRate = String.format("%.4f",  sectorOrderBuyAvgVal /sectorOrderSellAvgVal);
                        }
                        else if (sectorOrderSellAvgVal<0){
                            sectorOrderBSRateSource+="卖负";
                            sectorOrderBSRate = String.format("%.4f",sectorOrderBuyAvgVal/sectorOrderSellAvgVal);
                        }
                        else{
                            sectorOrderBSRateSource+="卖0";
                        }

                        String sectorWeightedOrderBSRate = "0";
                        Double sectorWeightedOrderBSRateValue = 0D;
                        String sectorWeightedOrderBSRateKey = ymdStr + "-" +sectorCode+"-WOBSRATE";

                        if (!sectorWeightedOrderSellAvgVal.equals(0D)){
                            sectorWeightedOrderBSRateValue = (double) sectorWeightedOrderBuyAvgVal /sectorWeightedOrderSellAvgVal;
                            sectorWeightedOrderBSRate = String.format("%.1f", sectorWeightedOrderBSRateValue);
                        }


//            计算5分钟压托比
                        String sectorOrderBSRateM5 = "0";
                        String sectorOrderBSRateM5Source="";
                        String sectorOrderBSRateM5Key = ymdStr + "-" +sectorCode+"-OBSRATEM5";
                        String sectorOrderBSRateM5SourceKey = ymdStr + "-" +sectorCode+"-OBSRATEM5S";
                        if (sector5mOrderBuyAvgVal>0){
                            sectorOrderBSRateM5Source+="买正";
                        }
                        else if (sector5mOrderBuyAvgVal<0){
                            sectorOrderBSRateM5Source+="买负";
                        }
                        else{
                            sectorOrderBSRateM5Source+="买0";
                        }

                        if (sector5mOrderSellAvgVal>0){
                            sectorOrderBSRateM5Source+="卖正";
                            sectorOrderBSRateM5 = String.format("%.4f", sector5mOrderBuyAvgVal /sector5mOrderSellAvgVal);
                        }
                        else if (sector5mOrderSellAvgVal<0){
                            sectorOrderBSRateM5Source+="卖负";
                            sectorOrderBSRateM5 = String.format("%.4f",sector5mOrderBuyAvgVal/sector5mOrderSellAvgVal);
                        }
                        else{
                            sectorOrderBSRateM5Source+="卖0";
                        }
                        realJedisInstance.set(sectorKeySDRate,sectorSDRateAvg);
                        realJedisInstance.set(sectorTransBsRateKey,sectorTransBsRate);
                        realJedisInstance.set(sectorOrderBSRateKey,sectorOrderBSRate);
                        realJedisInstance.set(sectorWeightedOrderBSRateKey,sectorWeightedOrderBSRate);
                        realJedisInstance.set(sectorOrderBSRateSourceKey,sectorOrderBSRateSource);
                        realJedisInstance.set(sectorOrderBSRateM5Key,sectorOrderBSRateM5);
                        realJedisInstance.set(sectorOrderBSRateM5SourceKey,sectorOrderBSRateM5Source);
                    }

                    Date finishTime = new Date();
                    System.out.println("已在 "+fullDF.format(finishTime)+" 完成更新行业"+sectorClass);
                }
                Thread.sleep(10000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (null != realJedisInstance) {
                realJedisInstance.close();
            }
        }
    }
}

package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

public class SectorStat {

    public static void getSectorStat(HashMap<String,Integer> stockRedisIndexMap, HashMap<String,Integer> sectorRedisIndexMap, String sector_class, HashMap<String,ArrayList<String>> swSectorListMap, HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection, JedisPool[] jedisPoolArray) throws ClassNotFoundException, SQLException, InstantiationException, ParseException, IllegalAccessException {
        Date statDate = new Date();
        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
        String ymdStr = ymdDF.format(statDate);
        ArrayList<String> sectorCodeList = swSectorListMap.get(sector_class+"Code");
        HashMap<String,ArrayList<String>> sectorStkMap = sectorStkMapCollection.get(sector_class+"Code");
        for (String sectorCode:sectorCodeList) {
            ArrayList<String> stkCodeList = sectorStkMap.get(sectorCode);
            String sectorKeyTransBuy = ymdStr + "-" +sectorCode+"-66-a";
            String sectorKeyTransSell = ymdStr + "-" +sectorCode+"-83-a";
            String sectorKeyOrderBuy = ymdStr + "-" +sectorCode+"-BO-a";
            String sectorKeyOrderSell = ymdStr + "-" +sectorCode+"-SO-a";
            String sector5mKeyOrderBuy = ymdStr + "-" +sectorCode+"-BO-5ma";
            String sector5mKeyOrderSell = ymdStr + "-" +sectorCode+"-SO-5ma";
            String sectorKeySDRate = ymdStr + "-" +sectorCode+"-SDRate";
            long sectorTransBuy = 0;
            long sectorTransSell = 0;
            long sectorOrderBuy = 0;
            long sectorOrderSell = 0;

            long sector5mOrderBuy = 0;
            long sector5mOrderSell = 0;
            double sectorTransBuyAvgVal = 0d;
            double sectorTransSellAvgVal = 0d;
            double sectorOrderBuyAvgVal = 0d;
            double sectorOrderSellAvgVal = 0d;

            double sector5mOrderBuyAvgVal = 0d;
            double sector5mOrderSellAvgVal =0d;
            double sectorSDRate = 0d;
            String sectorTransBuyAvg = "0";
            String sectorTransSellAvg = "0";
            String sectorOrderBuyAvg = "0";
            String sectorOrderSellAvg = "0";
            String sector5mOrderBuyAvg = "0";
            String sector5mOrderSellAvg = "0";
            String sectorSDRateAvg = "0";
            int stkCodeListSize = stkCodeList.size();
            for (int i = 0; i < stkCodeListSize; i++) {
                String stkCode = stkCodeList.get(i);
                Integer redisIndex = stockRedisIndexMap.get(stkCode);
                Jedis realJedisInstance = null;
                if(redisIndex!=null){
                    try {
                        // 从连接池获取jedis对象
                        realJedisInstance = jedisPoolArray[redisIndex].getResource();
                        String stkKeyTransBuy = ymdStr + "-" +stkCode+"-66-a";
                        String stkKeyTransSell = ymdStr + "-" +stkCode+"-83-a";
                        String stkKeyOrderBuy = ymdStr + "-" +stkCode+"-BO-a";
                        String stkKeyOrderSell = ymdStr + "-" +stkCode+"-SO-a";
                        String stk5mKeyOrderBuy = ymdStr + "-" +stkCode+"-BO-5ma";
                        String stk5mKeyOrderSell = ymdStr + "-" +stkCode+"-SO-5ma";
                        String stkKeySDRate = ymdStr + "-" +stkCode+"-SDRate";
                        String stkTransBuyValue = realJedisInstance.get(stkKeyTransBuy);
                        String stkTransSellValue = realJedisInstance.get(stkKeyTransSell);
                        String stkOrderBuyValue = realJedisInstance.get(stkKeyOrderBuy);
                        String stkOrderSellValue = realJedisInstance.get(stkKeyOrderSell);
                        String stk5mOrderBuyValue = realJedisInstance.get(stk5mKeyOrderBuy);
                        String stk5mOrderSellValue = realJedisInstance.get(stk5mKeyOrderSell);
                        String stkSDValue = realJedisInstance.get(stkKeySDRate);
                        if (stkTransBuyValue != null) {
                            sectorTransBuy+=Long.parseLong(stkTransBuyValue);
                        }
                        if (stkTransSellValue != null) {
                            sectorTransSell+=Long.parseLong(stkTransSellValue);
                        }
                        if (stkOrderBuyValue != null) {
                            sectorOrderBuy+=Long.parseLong(stkOrderBuyValue);
                        }
                        if (stkOrderSellValue != null) {
                            sectorOrderSell+=Long.parseLong(stkOrderSellValue);
                        }
                        if (stk5mOrderBuyValue != null) {
                            sector5mOrderBuy+=Long.parseLong(stk5mOrderBuyValue);
                        }
                        if (stk5mOrderSellValue != null) {
                            sector5mOrderSell+=Long.parseLong(stk5mOrderSellValue);
                        }
                        if (stkSDValue != null) {
                            sectorSDRate+=Double.parseDouble(stkSDValue);
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
            if(stkCodeListSize!=0){
                sectorTransBuyAvgVal = (double)sectorTransBuy/stkCodeListSize;
                sectorTransSellAvgVal = (double)sectorTransSell/stkCodeListSize;
                sectorOrderBuyAvgVal = (double)sectorOrderBuy/stkCodeListSize;
                sectorOrderSellAvgVal = (double)sectorOrderSell/stkCodeListSize;
                sector5mOrderBuyAvgVal = (double)sector5mOrderBuy/stkCodeListSize;
                sector5mOrderSellAvgVal = (double)sector5mOrderSell/stkCodeListSize;
                sectorTransBuyAvg = String.valueOf(sectorTransBuyAvgVal);
                sectorTransSellAvg = String.valueOf(sectorTransSellAvgVal);
                sectorOrderBuyAvg = String.valueOf(sectorOrderBuyAvgVal);
                sectorOrderSellAvg = String.valueOf(sectorOrderSellAvgVal);
                sector5mOrderBuyAvg = String.valueOf(sector5mOrderBuyAvgVal);
                sector5mOrderSellAvg = String.valueOf(sector5mOrderSellAvgVal);
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
            String sectorWeightedOrderBSRateKey = ymdStr + "-" +sectorCode+"-WOBSRATE";

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

            Jedis sectorJedisInstance = null;
            Integer sectorRedisIndex = sectorRedisIndexMap.get(sectorCode);
            if(sectorRedisIndex!=null){
                try {
                    // 从连接池获取jedis对象
                    sectorJedisInstance = jedisPoolArray[sectorRedisIndex].getResource();
                    sectorJedisInstance.set(sectorKeyTransBuy,sectorTransBuyAvg);
                    sectorJedisInstance.set(sectorKeyTransSell,sectorTransSellAvg);
                    sectorJedisInstance.set(sectorKeyOrderBuy,sectorOrderBuyAvg);
                    sectorJedisInstance.set(sectorKeyOrderSell,sectorOrderSellAvg);
                    sectorJedisInstance.set(sector5mKeyOrderBuy,sector5mOrderBuyAvg);
                    sectorJedisInstance.set(sector5mKeyOrderSell,sector5mOrderSellAvg);
                    sectorJedisInstance.set(sectorKeySDRate,sectorSDRateAvg);

                    sectorJedisInstance.set(sectorTransBsRateKey,sectorTransBsRate);
                    sectorJedisInstance.set(sectorOrderBSRateKey,sectorOrderBSRate);
                    sectorJedisInstance.set(sectorOrderBSRateSourceKey,sectorOrderBSRateSource);
                    sectorJedisInstance.set(sectorOrderBSRateM5Key,sectorOrderBSRateM5);
                    sectorJedisInstance.set(sectorOrderBSRateM5SourceKey,sectorOrderBSRateM5Source);

                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    if (null != sectorJedisInstance) {
                        sectorJedisInstance.close();
                    }
                }
            }
        }
    }
    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, ParseException {
//        getSectorStkMap("sw3",getSwSectorList("sw3"));
//        Jedis jedisInstance2 = new Jedis("127.0.0.1",6464,3000);
//        jedisInstance2.select(1);
//        ArrayList<String> sectorClassList = new ArrayList<String>();
//        sectorClassList.add("sw1");
//        sectorClassList.add("sw2");
//        sectorClassList.add("sw3");
//        Date statDate = new Date();
//        SimpleDateFormat ymdDF = new SimpleDateFormat("yyyyMMdd");
//        SimpleDateFormat fullDF = new SimpleDateFormat("yyyyMMdd HHmmss");
//        String ymdStr = ymdDF.format(statDate);
//        for (String sectorClass:sectorClassList) {
//            getSingleSectorStat(ymdStr,jedisInstance2,sectorClass);
//            Date finishTime = new Date();
//            System.out.println("已在 "+fullDF.format(finishTime)+" 完成更新"+sectorClass);
//        }
    }
}

package com.alphainv.tdfapi;


import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StockRankStat implements Runnable{

    ConcurrentHashMap<String,Double> weightedOrderBSRateMapAll;
    ConcurrentHashMap<String,Double> orderBsRateMapALL;
    ConcurrentHashMap<String,Double> transBsRateMapALL;
    ConcurrentHashMap<String, ArrayList<Double>> compositeScoreMapALL;
    ConcurrentHashMap<String,Integer> highLimitFlagMapAll;
    String insertStatTime;

    public StockRankStat(ConcurrentHashMap<String, Double> weightedOrderBSRateMapAll,
                         ConcurrentHashMap<String, Double> orderBsRateMapALL,
                         ConcurrentHashMap<String, Double> transBsRateMapALL,
                         ConcurrentHashMap<String, ArrayList<Double>> compositeScoreMapALL,
                         ConcurrentHashMap<String,Integer> highLimitFlagMapAll,
                         String insertStatTime) {
        this.weightedOrderBSRateMapAll = weightedOrderBSRateMapAll;
        this.orderBsRateMapALL = orderBsRateMapALL;
        this.transBsRateMapALL = transBsRateMapALL;
        this.compositeScoreMapALL = compositeScoreMapALL;
        this.insertStatTime = insertStatTime;
        this.highLimitFlagMapAll = highLimitFlagMapAll;
    }

    @Override
    public void run() {
        Date statDate = new Date();
        long statDateLong = statDate.getTime();
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String insertStatDate = insertStatTime.substring(0,8);
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
            String oracleUser = "iacore";
            String oraclePd = "tmd242209";
            Connection oracleCon = null;
            oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
            Statement oracleStatement = oracleCon.createStatement();
            ArrayList<List<Map.Entry<String,Double>>> insertTableSourceList = new ArrayList<>();
            List<Map.Entry<String,Double>> weightedOrderBSRateEntryList = MapOrderByValueUtil.getMapOrderByValue(weightedOrderBSRateMapAll);
            insertTableSourceList.add(weightedOrderBSRateEntryList);
            List<Map.Entry<String,Double>> orderBSRateEntryList = MapOrderByValueUtil.getMapOrderByValue(orderBsRateMapALL);
            insertTableSourceList.add(orderBSRateEntryList);
            List<Map.Entry<String,Double>> transBsRateEntryList = MapOrderByValueUtil.getMapOrderByValue(transBsRateMapALL);
            insertTableSourceList.add(transBsRateEntryList);

            String[] insertFieldNames = {"w_order_BS_Rate","order_BS_Rate","trans_Bs_Rate"};
            String[] insertTableNames = {"C_w_order_BS_Rate_RANK_STAT","C_order_BS_Rate_RANK_STAT","C_trans_Bs_Rate_RANK_STAT"};
            for(int i=0; i<insertFieldNames.length; i++){
                String insertFieldName = insertFieldNames[i];
                String insertFieldNameRank = insertFieldNames[i]+"_rank";
                String insertTableName = insertTableNames[i];
                List<Map.Entry<String,Double>> insertEntryList = insertTableSourceList.get(i);
                if (insertEntryList.size()>0){
                    int iterateIndex=1;
                    int iterateCountWithoutHL=1;
                    for (Map.Entry<String,Double> singleStkEntry:insertEntryList) {
                        if (iterateCountWithoutHL<51){
                            String stkCode = singleStkEntry.getKey();
                            Integer highLimitFlag = highLimitFlagMapAll.get(stkCode);
                            highLimitFlag = highLimitFlag != null?highLimitFlag:0;
                            String insertFieldNameValue = String.format("%.1f",singleStkEntry.getValue());
                            String insertFieldNameRankValue = String.valueOf(iterateIndex);
                            String insertSql = "insert into "+insertTableName+" (stk_code,"+insertFieldName+","+insertFieldNameRank+",stat_time,stat_date,high_limit_flag)" +
                                    " values('"+ stkCode +"',"+insertFieldNameValue+","+insertFieldNameRankValue+",to_date('"+insertStatTime+"', 'yyyy-mm-dd hh24:mi:ss'),to_date('"+insertStatDate+"', 'yyyy-mm-dd'),"+highLimitFlag+")";
//                        System.out.println(insertSql);
                            oracleStatement.executeUpdate(insertSql);
                            if (highLimitFlag.equals(0)){
                                iterateCountWithoutHL++;
                            }
                            iterateIndex++;
                        }
                        else {
                            break;
                        }
                    }
                }

            }

//        System.out.println(swSql);

            List<Map.Entry<String,ArrayList<Double>>> compositeScoreEntryList = MapOrderByValueUtil.getMapOrderByListValue(compositeScoreMapALL);
            int iterateIndex=1;
            int iterateCountWithoutHL=1;
            for (Map.Entry<String,ArrayList<Double>> compositeScoreEntry:compositeScoreEntryList) {
                if (iterateCountWithoutHL<51){
                    String stkCode = compositeScoreEntry.getKey();
                    Integer highLimitFlag = highLimitFlagMapAll.get(stkCode);
                    highLimitFlag = highLimitFlag != null?highLimitFlag:0;
                    ArrayList<Double> valueList = compositeScoreEntry.getValue();
                    String CScoreValue = String.format("%.1f",valueList.get(2));
                    String CScoreValueRankValue = String.valueOf(iterateIndex);
                    String weightedOrderBSRateValue = String.format("%.1f",valueList.get(0));
                    String transBSRateValue = String.format("%.1f",valueList.get(1));
                    String insertSql = "insert into C_SCORE_RANK_STAT (stk_code, c_score_rank, c_score, w_order_BS_Rate, trans_Bs_Rate, stat_time,stat_date,high_limit_flag)" +
                            "values('"+ stkCode +"',"+CScoreValueRankValue+","+CScoreValue+","+weightedOrderBSRateValue+","+transBSRateValue+",to_date('"+insertStatTime+"', 'yyyy-mm-dd hh24:mi:ss'),to_date('"+insertStatDate+"', 'yyyy-mm-dd'),"+highLimitFlag+")";
//                System.out.println(insertSql);
                    oracleStatement.executeUpdate(insertSql);
                    if (highLimitFlag.equals(0)){
                        iterateCountWithoutHL++;
                    }
                    iterateIndex++;
                }
                else {
                    break;
                }
            }
            oracleStatement.close();
            oracleCon.close();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        Date finishTime = new Date();
        long finishLong = finishTime.getTime();
        int threadNum = Thread.currentThread().getThreadGroup().activeCount();
        System.out.println(Thread.currentThread().getName()+"已在 "+fullDF.format(finishTime)+" 完成股票比例写入数据库操作；耗时："+(finishLong-statDateLong)+"毫秒, 当前线程池线程数："+threadNum);
    }
}

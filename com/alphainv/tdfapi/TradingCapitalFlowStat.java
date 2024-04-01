package com.alphainv.tdfapi;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class TradingCapitalFlowStat implements Runnable{
    ConcurrentHashMap<String,Long> transBuyMapAll;
    ConcurrentHashMap<String,Long> transSellMapAll;

    public TradingCapitalFlowStat(ConcurrentHashMap<String,Long> transBuyMapAll,
            ConcurrentHashMap<String,Long> transSellMapAll
            ) {
        this.transBuyMapAll = transBuyMapAll;
        this.transSellMapAll = transSellMapAll;
    }

    @Override
    public void run() {
        Date statDate = new Date();
        long statDateLong = statDate.getTime();
        SimpleDateFormat fullDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String insertStatTime = fullDF.format(statDate);
        String insertStatDate = insertStatTime.substring(0,10);
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd123";
        Connection oracleCon = null;
        Statement oracleStatement = null;

        try {
            oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
            oracleStatement = oracleCon.createStatement();
            for(Map.Entry<String, Long> entry : transBuyMapAll.entrySet()){
                String stkCode = entry.getKey();
                Long buyValue = entry.getValue();
                String buyValueStr = buyValue.toString();
                Long sellValue = transSellMapAll.get(stkCode);
                String sellValueStr = sellValue.toString();
                System.out.println("key = " + stkCode + ",buy value = " + buyValue+ ",sell value = " + sellValue);
                String insertSql = "insert into trading_capital_flow_l2 "+" (stk_code, buy_value, sell_value, stat_time, stat_date)" +
                        " values('"+ stkCode +"',"+buyValueStr+","+sellValueStr+",to_date('"+insertStatTime+"', 'yyyy-mm-dd hh24:mi:ss'),to_date('"+insertStatDate+"', 'yyyy-mm-dd'))";
//                      System.out.println(insertSql);
                oracleStatement.executeUpdate(insertSql);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        finally{
            try {
                if(oracleStatement!=null){
                    oracleStatement.close();
                }
                if(oracleCon!=null){
                    oracleCon.close();
                }
            } catch (SQLException sqlException) {
                sqlException.printStackTrace();
            }
        }

        Date finishTime = new Date();
        long finishLong = finishTime.getTime();
        int threadNum = Thread.currentThread().getThreadGroup().activeCount();
        System.out.println(Thread.currentThread().getName()+"已在 "+fullDF.format(finishTime)+" 完成股票资金净流入流出写入数据库操作；耗时："+(finishLong-statDateLong)+"毫秒, 当前线程池线程数："+threadNum);
    }
}

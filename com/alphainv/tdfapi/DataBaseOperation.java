package com.alphainv.tdfapi;

import redis.clients.jedis.Jedis;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class DataBaseOperation {
    public static Object[] getStockListCollection() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        Object[] stockRedisArray=new Object[2];
        //        Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
//        String url = "jdbc:jtds:sqlserver://10.23.188.51:1433/myStrategy";
//        //String url = "jdbc:jtds:sqlserver://localhost:1433/fds";
//        String user = "sa";
//        String password = "Wpy021138";
//        Connection conn = DriverManager.getConnection(url, user, password);
//        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
////        String sql = "select top 2843 * from (select row_number() over (partition by induCode order by wt_LS desc) as rankNum,* from MP_STK_TGT_Alpha) t";
//        String sql = "select * from (select row_number() over (partition by induCode order by wt_LS desc) as rankNum,* from MP_STK_TGT_Alpha) t";
//        ResultSet rs = stmt.executeQuery(sql);
        ArrayList<String> portfolioStocks = new ArrayList<String>();
        ArrayList<ArrayList<String>> listCollection = new ArrayList<ArrayList<String>>();
        ArrayList<Integer> redisIndexList = new ArrayList<Integer>();
//        while (rs.next()){
//            String stkCode = rs.getString("stkCode");
//            portfolioStocks.add(stkCode);
//            if (portfolioStocks.size()==200){
//                listCollection.add(portfolioStocks);
//                portfolioStocks = new ArrayList<String>();
//            }
//        }
//        if (portfolioStocks.size()<200){
//            listCollection.add(portfolioStocks);
//        }

        HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection = getSectorStkMapCollection(getSwSectorListMap());
        HashMap<String,ArrayList<String>> sectorStkMap = sectorStkMapCollection.get("sw1Code");
        portfolioStocks.addAll(sectorStkMap.get("230000"));
        portfolioStocks.addAll(sectorStkMap.get("460000"));
        portfolioStocks.addAll(sectorStkMap.get("510000"));
        portfolioStocks.addAll(sectorStkMap.get("480000"));
        listCollection.add(portfolioStocks);
        portfolioStocks = new ArrayList<String>();
        redisIndexList.add(0);
        portfolioStocks.addAll(sectorStkMap.get("210000"));
        portfolioStocks.addAll(sectorStkMap.get("330000"));
        listCollection.add(portfolioStocks);
        portfolioStocks = new ArrayList<String>();
        redisIndexList.add(0);
        portfolioStocks.addAll(sectorStkMap.get("610000"));
        portfolioStocks.addAll(sectorStkMap.get("490000"));
        listCollection.add(portfolioStocks);
        portfolioStocks = new ArrayList<String>();
        redisIndexList.add(0);
        portfolioStocks.addAll(sectorStkMap.get("450000"));
        portfolioStocks.addAll(sectorStkMap.get("110000"));
        listCollection.add(portfolioStocks);
        redisIndexList.add(0);
        listCollection.add(sectorStkMap.get("350000"));
        redisIndexList.add(0);
        listCollection.add(sectorStkMap.get("650000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("730000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("420000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("340000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("430000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("240000"));
        redisIndexList.add(1);
        listCollection.add(sectorStkMap.get("620000"));
        redisIndexList.add(2);
        listCollection.add(sectorStkMap.get("360000"));
        redisIndexList.add(2);
        listCollection.add(sectorStkMap.get("720000"));
        redisIndexList.add(2);
//        listCollection.add(sectorStkMap.get("410000"));
//        redisIndexList.add(2);
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("410000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(2);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(2);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("280000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(3);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(3);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("630000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(3);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(3);
        }
//        listCollection.add(sectorStkMap.get("280000"));
//        redisIndexList.add(3);
//        listCollection.add(sectorStkMap.get("630000"));
//        redisIndexList.add(3);

        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("710000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(3);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(3);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("270000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(4);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(4);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("220000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(4);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(4);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("370000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(5);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(5);
        }
        portfolioStocks = new ArrayList<String>();
        for (String stkCode:sectorStkMap.get("640000")) {
            portfolioStocks.add(stkCode);
            if (portfolioStocks.size()==100){
                listCollection.add(portfolioStocks);
                redisIndexList.add(5);
                portfolioStocks = new ArrayList<String>();
            }
        }
        if (portfolioStocks.size()<100){
            listCollection.add(portfolioStocks);
            redisIndexList.add(5);
        }
//        for (ArrayList<String> portfolioStocks2:listCollection) {
//            for (String stockCode2:portfolioStocks2) {
//                System.out.println(stockCode2);
//            }
//        }
        stockRedisArray[0] = listCollection;
        stockRedisArray[1] = redisIndexList;
        return stockRedisArray;
    }

    public static Object[] getStockListCollectionV2() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        Object[] stockRedisArray=new Object[2];
        ArrayList<String> portfolioStocks = new ArrayList<String>();
        ArrayList<ArrayList<String>> listCollection = new ArrayList<ArrayList<String>>();
        ArrayList<Integer> redisIndexList = new ArrayList<Integer>();
        HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection = getSectorStkMapCollection(getSwSectorListMap());
        HashMap<String,ArrayList<String>> sectorStkMap = sectorStkMapCollection.get("sw1Code");
        String[] sectorPart0CodeArray = {"510000","210000","330000","490000","450000","110000","350000","650000","730000"};
        String[] sectorPart1CodeArray = {"610000","340000","430000","240000","620000","720000"};
        String[] sectorPart2CodeArray = {"460000","360000","410000","270000"};
        String[] sectorPart3CodeArray = {"420000","280000","220000"};
        String[] sectorPart4CodeArray = {"480000","710000","370000"};
        String[] sectorPart5CodeArray = {"230000","630000","640000"};
        String[][] sectorCodeArrayCollection = {sectorPart0CodeArray,sectorPart1CodeArray,sectorPart2CodeArray,sectorPart3CodeArray,sectorPart4CodeArray,sectorPart5CodeArray};
        int redisIndex = 0;
        for (String[] sectorPartCodeArray:sectorCodeArrayCollection) {
            for (String singleSectorCode:sectorPartCodeArray) {
                portfolioStocks.addAll(sectorStkMap.get(singleSectorCode));
            }
            ArrayList<String> splitPortfolioStocks = new ArrayList<String>();
            for (String stkCode:portfolioStocks) {
                splitPortfolioStocks.add(stkCode);
                if (splitPortfolioStocks.size()==200){
                    listCollection.add(splitPortfolioStocks);
                    redisIndexList.add(redisIndex);
                    splitPortfolioStocks = new ArrayList<String>();
                }
            }
            if (splitPortfolioStocks.size()<200&splitPortfolioStocks.size()>0){
                listCollection.add(splitPortfolioStocks);
                redisIndexList.add(redisIndex);
            }
            portfolioStocks =  new ArrayList<String>();
            redisIndex++;
        }
        stockRedisArray[0] = listCollection;
        stockRedisArray[1] = redisIndexList;
        return stockRedisArray;
    }

    public static ArrayList<String> getSwSectorList(String sector_class) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        String sector_class_code = sector_class+"Code";
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        String swSql = "select distinct("+sector_class_code.toUpperCase()+") from CODE_STK_INDU_HSALL where "+sector_class_code.toUpperCase()+" is not null order by "+sector_class_code.toUpperCase();// 预编译语句，“？”代表参数
//        System.out.println(swSql);
        PreparedStatement pre = oracleCon.prepareStatement(swSql);// 实例化预编译语句
        ResultSet swResult = pre.executeQuery();
        ArrayList<String> sectorCodeList = new ArrayList<String>();
        while (swResult.next()){
            String sectorCode = swResult.getString(sector_class_code);
//            System.out.println(sectorCode);
            sectorCodeList.add(sectorCode);
        }
        return sectorCodeList;
    }

    public static HashMap<String,ArrayList<String>> getSwSectorListMap() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        HashMap<String,ArrayList<String>> swSectorListMap = new HashMap<String,ArrayList<String>>();
        String[] sector_class_array = {"sw1Code", "sw2Code", "sw3Code"};
        for (String sector_class_code:sector_class_array) {
            String swSql = "select distinct("+sector_class_code.toUpperCase()+") from CODE_STK_INDU_HSALL where "+sector_class_code.toUpperCase()+" is not null order by "+sector_class_code.toUpperCase();// 预编译语句，“？”代表参数
//        System.out.println(swSql);
            PreparedStatement pre = oracleCon.prepareStatement(swSql);// 实例化预编译语句
            ResultSet swResult = pre.executeQuery();
            ArrayList<String> sectorCodeList = new ArrayList<String>();
            while (swResult.next()){
                String sectorCode = swResult.getString(sector_class_code);
//            System.out.println(sectorCode);
                sectorCodeList.add(sectorCode);
            }
            swSectorListMap.put(sector_class_code,sectorCodeList);
        }
        return swSectorListMap;
    }

    public static HashMap<String,ArrayList<String>> getSectorStkMap(String sector_class, ArrayList<String> sectorCodeList) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        HashMap<String,ArrayList<String>> sectorStkMap = new HashMap<String,ArrayList<String>>();
        String sector_class_code = sector_class+"Code";
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        String swSql = "select * from CODE_STK_INDU_HSALL where "+sector_class_code.toUpperCase()+" is not null order by "+sector_class_code.toUpperCase();// 预编译语句，“？”代表参数
        System.out.println(swSql);
        PreparedStatement pre = oracleCon.prepareStatement(swSql);// 实例化预编译语句
        ResultSet swResult = pre.executeQuery();
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        String sectorCode = sectorCodeList.get(0);
        ArrayList<String> stkList = new ArrayList<String>();
        while (swResult.next()){
            String sectorCodeResult = swResult.getString(sector_class_code);
            if (!sectorCodeResult.equals(sectorCode)){
                sectorStkMap.put(sectorCode,stkList);
                sectorCode = sectorCodeResult;
                stkList = new ArrayList<String>();
            }
            String stkCode = swResult.getString("stkCode");
//            System.out.println(sectorCode+":"+stkCode);
            stkList.add(stkCode);
        }
        if (stkList.size()>0){
            sectorStkMap.put(sectorCode,stkList);
        }
        return sectorStkMap;
    }

    public static ArrayList<String> getallStkList() throws ClassNotFoundException, SQLException {
        ArrayList<String> stkCodeList = new ArrayList<String>();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        String allStkSql = "select * from CODE_STK_INDU_HSALL";// 预编译语句，“？”代表参数
//        System.out.println(swSql);
        PreparedStatement pre = oracleCon.prepareStatement(allStkSql);// 实例化预编译语句
        ResultSet stkListResult = pre.executeQuery();
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        while (stkListResult.next()){
            String stkCode = stkListResult.getString("stkCode");
            stkCodeList.add(stkCode);
        }
        return stkCodeList;
    }



    public static HashMap<String,HashMap<String,ArrayList<String>>> getSectorStkMapCollection(HashMap<String,ArrayList<String>> swSectorListMap) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection = new HashMap<String,HashMap<String,ArrayList<String>>>();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        String[] sector_class_array = {"sw1Code", "sw2Code", "sw3Code"};
        for (String sector_class_code:sector_class_array) {
            HashMap<String,ArrayList<String>> sectorStkMap = new HashMap<String,ArrayList<String>>();
            String swSql = "select * from CODE_STK_INDU_HSALL where "+sector_class_code.toUpperCase()+" is not null order by "+sector_class_code.toUpperCase();// 预编译语句，“？”代表参数
//            System.out.println(swSql);
            PreparedStatement pre = oracleCon.prepareStatement(swSql);// 实例化预编译语句
            ResultSet swResult = pre.executeQuery();
            ArrayList<String> sectorCodeList = swSectorListMap.get(sector_class_code);
            String sectorCode = sectorCodeList.get(0);
            ArrayList<String> stkList = new ArrayList<String>();
            while (swResult.next()){
                String sectorCodeResult = swResult.getString(sector_class_code);
                if (!sectorCodeResult.equals(sectorCode)){
                    sectorStkMap.put(sectorCode,stkList);
                    sectorCode = sectorCodeResult;
                    stkList = new ArrayList<String>();
                }
                String stkCode = swResult.getString("stkCode");
//            System.out.println(sectorCode+":"+stkCode);
                stkList.add(stkCode);
            }
            if (stkList.size()>0){
                sectorStkMap.put(sectorCode,stkList);
            }
            sectorStkMapCollection.put(sector_class_code,sectorStkMap);
        }
        return sectorStkMapCollection;
    }

    public static HashSet<String> getAvailableStkSet() throws SQLException, ClassNotFoundException {
        HashSet<String> availableStkSet = new HashSet<>();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd242209";
        Connection oracleCon = DriverManager.getConnection(oracleUrl, oracleUser, oraclePd);
        String allStkSql = "select * from CODE_STK_INDU_HSALL";// 预编译语句，“？”代表参数
//        System.out.println(swSql);
        PreparedStatement pre = oracleCon.prepareStatement(allStkSql);// 实例化预编译语句
        ResultSet stkListResult = pre.executeQuery();
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        while (stkListResult.next()){
            String stkCode = stkListResult.getString("stkCode");
            availableStkSet.add(stkCode);
        }
        return availableStkSet;
    }

    public static void main(String[] args) throws ClassNotFoundException, SQLException, InstantiationException, IllegalAccessException, ParseException {
//        getSectorRedisIndexMap();
    }
}

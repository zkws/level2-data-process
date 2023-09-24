package com.alphainv.tdfapi;

import java.sql.*;
import java.text.ParseException;
import java.util.*;

public class DataBaseOperation {
        public static HashMap<String,ArrayList<String>> getSwSectorListMap() throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd123";
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
            pre.close();
        }
        oracleCon.close();
        return swSectorListMap;
    }

    public static HashMap<String,HashMap<String,ArrayList<String>>> getSectorStkMapCollection(HashMap<String,ArrayList<String>> swSectorListMap) throws ClassNotFoundException, IllegalAccessException, InstantiationException, SQLException, ParseException {
        HashMap<String,HashMap<String,ArrayList<String>>> sectorStkMapCollection = new HashMap<String,HashMap<String,ArrayList<String>>>();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd123";
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
            pre.close();
        }
        oracleCon.close();
        return sectorStkMapCollection;
    }

    public static HashSet<String> getAvailableStkSet() throws SQLException, ClassNotFoundException {
        HashSet<String> availableStkSet = new HashSet<>();
        Class.forName("oracle.jdbc.driver.OracleDriver");
        String oracleUrl = "jdbc:oracle:thin:@//10.23.188.53:1521/ORCL";
        String oracleUser = "iacore";
        String oraclePd = "tmd123";
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
        pre.close();
        oracleCon.close();
        return availableStkSet;
    }

    public static HashSet<String> getTop20PercentStkSet() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HashSet<String> top20PercentStkSet = new HashSet<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
        String url = "jdbc:jtds:sqlserver://10.23.188.51:1433/myStrategy";
        String user = "sa";
        String password = "Wpy021138";
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        String allPorttolioSql = "select * from (select row_number() over (partition by induCode order by wt_LS desc) as rankNum, count(induCode) over (partition by induCode) as partition_num, * from MP_STK_TGT_Alpha) t where t.rankNum < t.partition_num*0.2 or t.rankNum < 4;";
        ResultSet allPorttolioResult = stmt.executeQuery(allPorttolioSql);
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        while (allPorttolioResult.next()){
            String stkCode = allPorttolioResult.getString("stkCode");
            top20PercentStkSet.add(stkCode);
        }
        stmt.close();
        conn.close();
        return top20PercentStkSet;
    }

    public static HashSet<String> getSelectedPortfolioStkSet() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HashSet<String> selectedPortfolioStkSet = new HashSet<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
        String url = "jdbc:jtds:sqlserver://10.23.188.51:1433/myStrategy";
        String user = "sa";
        String password = "Wpy021138";
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        String porttolioSql = "select distinct(stk_Code) from mp_stk_select;";
        ResultSet porttolioResult = stmt.executeQuery(porttolioSql);
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        while (porttolioResult.next()){
            String stkCode = porttolioResult.getString("stk_Code");
//            System.out.println(stkCode);
            selectedPortfolioStkSet.add(stkCode);
        }
        stmt.close();
        conn.close();
        return selectedPortfolioStkSet;
    }

    public static HashSet<String> getBoughtPortfolioStkSet() throws SQLException, ClassNotFoundException, IllegalAccessException, InstantiationException {
        HashSet<String> boughtPortfolioStkSet = new HashSet<>();
        Class.forName("net.sourceforge.jtds.jdbc.Driver").newInstance();
        String url = "jdbc:jtds:sqlserver://10.23.188.51:1433/myStrategy";
        String user = "sa";
        String password = "Wpy021138";
        Connection conn = DriverManager.getConnection(url, user, password);
        Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        String porttolioSql = "select distinct(stk_code) from MP_STK_PORTFOLIO where stk_code is not null and LEN(stk_code)>0;";
        ResultSet porttolioResult = stmt.executeQuery(porttolioSql);
//        ArrayList<String> sectorStkCodeList = new ArrayList<String>();
        while (porttolioResult.next()){
            String stkCode = porttolioResult.getString("stk_Code");
//            System.out.println(stkCode);
            boughtPortfolioStkSet.add(stkCode);
        }
        stmt.close();
        conn.close();
        return boughtPortfolioStkSet;
    }

    public static void main(String[] args){
//        try {
//            getSelectedPortfolioStkSet();
//        } catch (SQLException throwables) {
//            throwables.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        } catch (IllegalAccessException e) {
//            e.printStackTrace();
//        } catch (InstantiationException e) {
//            e.printStackTrace();
//        }
    }
}

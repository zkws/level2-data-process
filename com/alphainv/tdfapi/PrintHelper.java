package com.alphainv.tdfapi;

//import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.GregorianCalendar;
import java.util.Calendar;

import cn.com.wind.td.tdf.TDFClient;
import cn.com.wind.td.tdf.TDF_ADD_CODE;
import cn.com.wind.td.tdf.TDF_BBQBID_DATA;
import cn.com.wind.td.tdf.TDF_BBQTRANSACTION;
import cn.com.wind.td.tdf.TDF_BROKER_QUEUE;
import cn.com.wind.td.tdf.TDF_CODE;
import cn.com.wind.td.tdf.TDF_CODE_RESULT;
import cn.com.wind.td.tdf.TDF_CONNECT_RESULT;
import cn.com.wind.td.tdf.TDF_FUTURE_DATA;
import cn.com.wind.td.tdf.TDF_INDEX_DATA;
import cn.com.wind.td.tdf.TDF_LOGIN_RESULT;
import cn.com.wind.td.tdf.TDF_MARKET_DATA;
import cn.com.wind.td.tdf.TDF_MSG_DATA;
import cn.com.wind.td.tdf.TDF_MSG_ID;
import cn.com.wind.td.tdf.TDF_OPTION_CODE;
import cn.com.wind.td.tdf.TDF_ORDER;
import cn.com.wind.td.tdf.TDF_ORDER_QUEUE;
import cn.com.wind.td.tdf.TDF_TRANSACTION;

public class PrintHelper {

	static final HashMap<Integer, String> typeMap;
	
	static  ArrayList<DataInfo> dataMsgs;
	static  ArrayList<DataInfo> dataMsgsBackup;
	static String CurrrentDate; //只取一次
	//行情
	static WriteDataHelper wdhMarketData;
	//期货行情.csv
	static WriteDataHelper wdhFutureData;
	//期权.csv
	static WriteDataHelper wdhOptionData;
	//委托队列.csv
	static WriteDataHelper wdhOrderQueueData;
	//逐笔委托.csv
	static WriteDataHelper wdhOrderData;
	//逐笔成交.csv
	static WriteDataHelper wdhTransactionData;
	//指数.csv
	static WriteDataHelper wdhIndexData;
	//BBQ成交.csv
	static WriteDataHelper wdhBbqTransaction;
	//BBQ报价.csv
	static WriteDataHelper wdhBbqBid;
	//经纪商队列(HK).csv
	static WriteDataHelper wdhBrokerQueue;
	
	static {
		typeMap = new HashMap<Integer, String>();
		typeMap.put(TDF_MSG_ID.MSG_SYS_DISCONNECT_NETWORK   , "MSG_SYS_DISCONNECT_NETWORK");
		typeMap.put(TDF_MSG_ID.MSG_SYS_CONNECT_RESULT       , "MSG_SYS_CONNECT_RESULT");
		typeMap.put(TDF_MSG_ID.MSG_SYS_LOGIN_RESULT         , "MSG_SYS_LOGIN_RESULT");
		typeMap.put(TDF_MSG_ID.MSG_SYS_CODETABLE_RESULT     , "MSG_SYS_CODETABLE_RESULT");
		typeMap.put(TDF_MSG_ID.MSG_SYS_QUOTATIONDATE_CHANGE , "MSG_SYS_QUOTATIONDATE_CHANGE");
		typeMap.put(TDF_MSG_ID.MSG_SYS_MARKET_CLOSE         , "MSG_SYS_MARKET_CLOSE");
		typeMap.put(TDF_MSG_ID.MSG_SYS_HEART_BEAT           , "MSG_SYS_HEART_BEAT");
		typeMap.put(TDF_MSG_ID.MSG_DATA_INDEX               , "MSG_DATA_INDEX");
		typeMap.put(TDF_MSG_ID.MSG_DATA_MARKET              , "MSG_DATA_MARKET");
		typeMap.put(TDF_MSG_ID.MSG_DATA_FUTURE              , "MSG_DATA_FUTURE");
		typeMap.put(TDF_MSG_ID.MSG_DATA_TRANSACTION         , "MSG_DATA_TRANSACTION");
		typeMap.put(TDF_MSG_ID.MSG_DATA_ORDERQUEUE          , "MSG_DATA_ORDERQUEUE");
		typeMap.put(TDF_MSG_ID.MSG_DATA_ORDER               , "MSG_DATA_ORDER" );	
		typeMap.put(TDF_MSG_ID.MSG_DATA_BBQTRANSACTION		, "MSG_DATA_BBQTRANSACTION");
		typeMap.put(TDF_MSG_ID.MSG_DATA_BBQBID				, "MSG_DATA_BBQBID");
		typeMap.put(TDF_MSG_ID.MSG_DATA_NB_ORDER			, "MSG_DATA_NB_ORDER");
		typeMap.put(TDF_MSG_ID.MSG_DATA_NB_TRANSACTION		, "MSG_DATA_NB_TRANSACTION");
		
		Calendar todaysDate = new GregorianCalendar();    
		int year = todaysDate.get(Calendar.YEAR);   
		int month = todaysDate.get(Calendar.MONTH)   +   1;     
		int day = todaysDate.get(Calendar.DAY_OF_MONTH);
		CurrrentDate = String.valueOf(year*10000+month*100+day);
		dataMsgs = new ArrayList<DataInfo>();
		dataMsgsBackup = new  ArrayList<DataInfo>();
		try{
			//行情			
			String[] headerMarket ={"日期", "本地时间", "服务器时间", "交易所时间", "万得代码", "原始代码",
					"业务发生日(自然日)", "交易日","状态","前收","开盘价","最高价","最低价","最新价","申卖价",
					"申卖量","申买价","申买量","成交笔数","成交总量","成交总金额","委托买入总量","委托卖出总量",
					"加权平均委买价格","加权平均委卖价格","IOPV净值估值","到期收益率","涨停价","跌停价",
					"证券信息前缀","市盈率1","市盈率2","升跌2（对比上一笔）","交易标志","盘后价格","盘后量","盘后成交金额","盘后成交笔数"};
			String[] enHeaderMarket = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code",
					"ActionDay", "TradingDay", "Status", "PreClose", "Open", "High", "Low", "Match", "AskPrice",
					"AskVol", "BidPrice", "BidVol", "NumTrades", "Volume", "Turnover", "TotalBidVol", "TotalAskVol",
					"WeightedAvgBidPrice", "WeightedAvgAskPrice", "IOPV", "YieldToMaturity", "HighLimited", "LowLimited",
					"Prefix", "Syl1", "Syl2", "SD2", "TradeFlag", "AfterPrice", "AfterVolume", "AfterTurnover", "AfterMatchItems"
			};
			wdhMarketData = new WriteDataHelper(headerMarket, "market.csv");
			wdhMarketData.WriteRecordToFile(enHeaderMarket);
			//期货行情
			String[] headerFuture ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"交易日","状态","昨持仓","昨收盘价","昨结算","开盘价","最高价","最低价","最新价",
					"成交总量","成交总金额","持仓总量","今收盘","今结算","涨停价","跌停价","昨虚实度",
					"今虚实度","申卖价","申卖量","申买价","申买量","期货原始代码"};
			String[] enHeaderFuture = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"TradingDay", "Status", "PreOpenInterest", "PreClose", "PreSettlePrice", "Open", "High", "Low", "Match",
					"Volume", "Turnover", "OpenInterest", "Close", "SettlePrice", "HighLimited", "LowLimited", "PreDelta",
					"CurrDelta", "AskPrice", "AskVol", "BidPrice", "BidVol", "OriginalCode"
			};
			wdhFutureData = new WriteDataHelper(headerFuture, "future.csv");
			wdhFutureData.WriteRecordToFile(enHeaderFuture);
			//期权
			String[] headerOption ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"交易日","状态","昨持仓","昨收盘价","昨结算","开盘价","最高价","最低价","最新价",
					"成交总量","成交总金额","持仓总量","今收盘","今结算","涨停价","跌停价","昨虚实度",
					"今虚实度","申卖价","申卖量","申买价","申买量","期权合约代码","标的证券代码",
					"认购认沽","行权日"};
			String[] enHeaderOption = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"TradingDay", "Status", "PreOpenInterest", "PreClose", "PreSettlePrice", "Open", "High", "Low", "Match",
					"Volume", "Turnover", "OpenInterest", "Close", "SettlePrice", "HighLimited", "LowLimited", "PreDelta",
					"CurrDelta", "AskPrice", "AskVol", "BidPrice", "BidVol", "ContractID", "UnderlyingSecurityID",
					"CallOrPut", "ExerciseDate"
			};
			wdhOptionData = new WriteDataHelper(headerOption, "option.csv");
			wdhOptionData.WriteRecordToFile(enHeaderOption);
			//委托队列

			String[] headerOQ ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"买卖方向","成交价格","订单数量","明细个数","订单明细"};
			String[] enHeaderOQ = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"Side", "Price", "Orders", "ABItems", "ABItem"
			};
			wdhOrderQueueData = new WriteDataHelper(headerOQ, "orderqueue.csv");
			wdhOrderQueueData.WriteRecordToFile(enHeaderOQ);
			//逐笔委托

			String[] headerOrder ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"委托号", "委托价格", "委托数量", "委托类别", "委托代码", "Channel ID",
					"OrderOriNo", "BizIndex"};
			String[] enHeaderOrder = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"Order", "Price", "Volume", "OrderKind", "FunctionCode", "Channel ID",
					"OrderOriNo", "BizIndex"
			};
			wdhOrderData = new WriteDataHelper(headerOrder, "order.csv");
			wdhOrderData.WriteRecordToFile(enHeaderOrder);
			//逐笔成交

			String[] headerTransaction ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"成交编号","成交价格","成交数量","成交金额","买卖方向","成交类别","成交代码",
					"卖方委托序号","买方委托序号", "Channel ID", "BizIndex"};
			String[] enHeaderTransaction = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"Index", "Price", "Volume", "Turnover", "BSFlag", "OrderKind", "FunctionCode",
					"AskOrder", "BidOrder", "Channel ID", "BizIndex"
			};
			wdhTransactionData = new WriteDataHelper(headerTransaction, "transaction.csv");
			wdhTransactionData.WriteRecordToFile(enHeaderTransaction);
			//指数
			String[] headerIndex ={"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
					"交易日","状态","今开盘指数","最高指数","最低指数","最新指数","成交总量","成交总金额","前盘指数"};
			String[] enHeaderIndex = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "Code", "ActionDay",
					"TradingDay", "Status", "OpenIndex", "HighIndex", "LowIndex", "LastIndex", "TotalVolume", "Turnover","PreCloseIndex"
			};
			wdhIndexData = new WriteDataHelper(headerIndex, "index.csv");
			wdhIndexData.WriteRecordToFile(enHeaderIndex);
			//BBQ成交
			String[] headerBBQTrans ={"日期","本地时间","服务器时间","交易所时间","万得代码","业务发生日(自然日)","成交报价编号",
					"成交价格/收益率","收益率/价格标识","删除/成交","成交方向","报价机构","行权/到期"};
			String[] enHeaderBBQTrans = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "ActionDay", "DoneID",
					"Price", "PriceStatus", "Status", "Direction", "Source", "SpecialFlag"
			};
			wdhBbqTransaction = new WriteDataHelper(headerBBQTrans, "BBQTransaction.csv");
			wdhBbqTransaction.WriteRecordToFile(enHeaderBBQTrans);
			//BBQ报价
			String[] headerBBQBid ={"日期","本地时间","服务器时间","报价时间","万得代码","报价日(自然日)","报价机构",
					"买入收益率/价格","买入量","买入收益率/价格标识","是否bid","行权/到期","买入最优报价状态",
					"卖出收益率/价格","卖出量","卖出收益率/价格标识","是否ofr","行权/到期","卖出最优报价状态","成交报价编号"};
			String[] enHeaderBBQBid = {"CurrentDate","CurrentTime", "APITime", "Time", "WindCode", "ActionDay", "Source",
					"BidPrice", "BidVolume", "BidPriceStatus", "IsBid", "BidSpecialFlag", "BidStatus",
					"OfrPrice", "OfrVolume", "OfrPriceStatus", "IsOfr", "OfrSpecialFlag", "OfrStatus", "DoneID"
			};
			wdhBbqBid = new WriteDataHelper(headerBBQBid, "BBQBid.csv");
			wdhBbqBid.WriteRecordToFile(enHeaderBBQBid);

			//经纪商队列(HK)
			String[] headerBQ ={"日期","本地时间","服务器时间","万得代码","原始代码","业务发生日(自然日)",
					"叫卖时间","叫买时间","叫卖经纪个数","叫买经纪个数","叫卖前40经纪","叫买前40经纪"};
			String[] enHeaderBQ = {"CurrentDate", "CurrentTime", "APITime", "WindCode", "Code", "ActionDay",
					"AskTime", "BidTime", "AskBrokers", "BidBrokers", "AskBrokers", "BidBrokers"
			};
			wdhBrokerQueue = new WriteDataHelper(headerBQ, "BrokerQueue.csv");
			wdhBrokerQueue.WriteRecordToFile(enHeaderBQ);
			
		}catch (Exception e) {
		    e.printStackTrace(); 
		    System.exit(0);
		}
	}
	static String GetCurrentTime(){		
		Calendar todaysDate = new GregorianCalendar();    
		return  String.valueOf(  todaysDate.get(Calendar.HOUR_OF_DAY)*10000000 + 
				                 todaysDate.get(Calendar.MINUTE)*100000 + 
				                 todaysDate.get(Calendar.SECOND)*1000 + 
				                 todaysDate.get(Calendar.MILLISECOND) );
	}

	static String getDataTypeString(int type) {
		if (typeMap.containsKey(type))
			return typeMap.get(type);
		else
			return "Unknown data type [" + type + "]";
	}

	static void printCodeTable(TDF_CODE[] codes) {
		
		for ( TDF_CODE code : codes ) {
			StringBuffer sb = new StringBuffer();
			sb.append("TDF_CODE: ")
			  .append(code.getWindCode()).append(" ")
			  .append(code.getMarket()).append(" ")
			  .append(code.getCode()).append(" ")
			  .append(code.getENName()).append(" ")
			  .append(code.getCNName()).append(" ")
		      .append(code.getType());
			
			System.out.println(sb.toString());
		}		
	}
	

	static String arrayToStr(long[] array) {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for(long v: array) {
			sb.append(v).append(",");
		}
		sb.append("]");
		return sb.toString();
	}

	static String arrayToStr(int[] array, int count) {
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i = 0; i<count; i++) {
			int v = array[i];
			sb.append(v).append(",");
		}
		sb.append("]");
		return sb.toString();
	}

	static String arrayToStr(String[] array, int count) {
		if (array==null)
			return "[]";
		
		StringBuffer sb = new StringBuffer();
		sb.append("[");
		for (int i=0; i<count; i++) {
			String v = array[i];
			if (v!=null && v.length()>0)
				sb.append(v).append(",");
		}		
		sb.append("]");
		return sb.toString();
	}
	static Boolean IsListFull(int len){
		if (dataMsgs.size() > len){
			return true;
		}
		return false;
	}
    static void SaveData(DataInfo msg ){
    	synchronized (PrintHelper.class){  
    	    dataMsgs.add(msg);
    	}
    }
    static void CloseDataFiles(){
		wdhMarketData.Close();
		//期货行情.csv
		wdhFutureData.Close();
		//期权.csv
		wdhOptionData.Close();
		//委托队列.csv
		wdhOrderQueueData.Close();
		//逐笔委托.csv
		wdhOrderData.Close();
		//逐笔成交.csv
		wdhTransactionData.Close();
		//指数.csv
		wdhIndexData.Close();  	
		//BBQ成交.csv
		wdhBbqTransaction.Close();
		//BBQ报价.csv
		wdhBbqBid.Close();
		//经纪商队列(HK).csv
		wdhBrokerQueue.Close();
    }
	static void WriteData(Demo demo){
		long startTime = System.currentTimeMillis();
		int dataSize = dataMsgs.size();
		if (dataMsgsBackup.size() > 0){
			System.out.println("Error: dataMsgsBackup is not empty, clear it!");
			dataMsgsBackup.clear();		
		}
		synchronized (PrintHelper.class){ //交换，同步
		    ArrayList<DataInfo> temp = dataMsgsBackup;		
		    dataMsgsBackup = dataMsgs;
		    dataMsgs = temp;
		}
    	for (DataInfo data : dataMsgsBackup) {
    		switch(data.getMsg().getDataType()) {
	    		case TDF_MSG_ID.MSG_DATA_MARKET:			
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputDataMarket(concreteData.getMarketData(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_INDEX:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputIndexData(concreteData.getIndexData(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_FUTURE:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputFutureData(concreteData.getFutureData(),data.getMsg().getServerTime(),data.getLocalCurrentTime(), demo);				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_TRANSACTION:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputTransaction(concreteData.getTransaction(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_ORDERQUEUE:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputOrderQueue(concreteData.getOrderQueue(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_ORDER:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputOrder(concreteData.getOrder(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_BBQTRANSACTION:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputBbqTransaction(concreteData.getBbqtransaction(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_BBQBID:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputBbqBid(concreteData.getBbqbid(),data.getMsg().getServerTime(),data.getLocalCurrentTime());				
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_BROKERQUEUE:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i); 
						PrintHelper.OutputBrokerQueue(concreteData.getBrokerQueue(),
								data.getMsg().getServerTime(),data.getLocalCurrentTime());
					}							
					break;
				case TDF_MSG_ID.MSG_DATA_NB_TRANSACTION:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i);
						PrintHelper.OutputTransaction(concreteData.getNBTransaction().getTrans(),
								data.getMsg().getServerTime(),data.getLocalCurrentTime());
					}
					break;
				case TDF_MSG_ID.MSG_DATA_NB_ORDER:
					for (int i=0; i<data.getMsg().getAppHead().getItemCount(); i++) {
						TDF_MSG_DATA concreteData = TDFClient.getMessageData(data.getMsg(), i);
						PrintHelper.OutputOrder(concreteData.getNBOrder().getOrder(),
								data.getMsg().getServerTime(),data.getLocalCurrentTime());
					}
					break;
				default:
					break;
			}
    	}	
    	dataMsgsBackup.clear();		
		//行情
		wdhMarketData.WriteDataToFile();
		//期货行情.csv
		wdhFutureData.WriteDataToFile();
		//期权.csv
		wdhOptionData.WriteDataToFile();
		//委托队列.csv
		wdhOrderQueueData.WriteDataToFile();
		//逐笔委托.csv
		wdhOrderData.WriteDataToFile();
		//逐笔成交.csv
		wdhTransactionData.WriteDataToFile();
		//指数.csv
		wdhIndexData.WriteDataToFile();
		//BBQ成交.csv
		wdhBbqTransaction.WriteDataToFile();
		//BBQ报价.csv
		wdhBbqBid.WriteDataToFile();
		//经纪商队列.csv
		wdhBrokerQueue.WriteDataToFile();
		System.out.printf("WriteData: List Size = %d, Spend Time=%d\n", dataSize,System.currentTimeMillis() - startTime );		
	}	
	
	/*************************************Market Data***************************************/
	static void OutputDataMarket(TDF_MARKET_DATA data, int serverTime, String localCurrentTime){            
/*

 		     String[] header ={"日期", "本地时间", "服务器时间", "交易所时间", "万得代码", "原始代码", 
		    	"业务发生日(自然日)", "交易日","状态","前收","开盘价","最高价","最低价","最新价","申卖价",
		    "申卖量","申买价","申买量","成交笔数","	成交总量","成交总金额","委托买入总量","委托卖出总量",
		    	"加权平均委买价格","加权平均委卖价格","IOPV净值估值","到期收益率","涨停价","跌停价",
		    	"证券信息前缀","市盈率1","市盈率2","升跌2（对比上一笔）"};

* */
		
        String[] contents = {CurrrentDate,
        		             localCurrentTime,
    			             String.valueOf(serverTime),
    			             String.valueOf(data.getTime()),
    			             data.getWindCode(),
    			             data.getCode(),
    			             String.valueOf(data.getActionDay()),
    			             String.valueOf(data.getTradingDay()),        			             
    			             String.valueOf(data.getStatus()),
    			             String.valueOf(data.getPreClose()),
    			             String.valueOf(data.getOpen()),
    			             String.valueOf(data.getHigh()),
    			             String.valueOf(data.getLow()),
    			             String.valueOf(data.getMatch()),
    			             arrayToStr(data.getAskPrice()),
    			             arrayToStr(data.getAskVol()),
    			             arrayToStr(data.getBidPrice()),
    			             arrayToStr(data.getBidVol()),
    			             String.valueOf(data.getNumTrades()),
    			             String.valueOf(data.getVolume()),
    			             String.valueOf(data.getTurnover()),
    			             String.valueOf(data.getTotalBidVol()),
    			             String.valueOf(data.getTotalAskVol()),
    			             String.valueOf(data.getWeightedAvgBidPrice()),
    			             String.valueOf(data.getWeightedAvgAskPrice()),
    			             String.valueOf(data.getIOPV()),
    			             String.valueOf(data.getYieldToMaturity()),
    			             String.valueOf(data.getHighLimited()),
    			             String.valueOf(data.getLowLimited()),
    			             data.getPrefix(),
    			             String.valueOf(data.getSyl1()),
    			             String.valueOf(data.getSyl2()),
    			             String.valueOf(data.getSD2()),
    			             String.valueOf(data.getTradeFlag()),
    			             String.valueOf(data.getAfterPrice()),
    			             String.valueOf(data.getAfterVolume()),
    			             String.valueOf(data.getAfterTurnover()),
    			             String.valueOf(data.getAfterMatchItems())
		                         
        };        
        //wdhMarketData.WriteRecordToFile(contents);
        wdhMarketData.addRecord(contents);
		//在此方法处写将读到的数据输出到redis
	}
	static void printDataMarket(TDF_MARKET_DATA data){
		System.out.println("TDF_MARKET_DATA");
		StringBuilder sb = new StringBuilder();
		sb.append("          SzWindCode = " + data.getWindCode() + "\n")
		  .append("          nActionDay = " + data.getActionDay() + "\n")
		  .append("         nTradingDay = " + data.getTradingDay() + "\n")
		  .append("               nTime = " + data.getTime() + "\n")
		  .append("             nStatus = " + data.getStatus() + "\n")
		  .append("           nPreClose = " + data.getPreClose() + "\n")
		  .append("               nOpen = " + data.getOpen() + "\n")
		  .append("               nHigh = " + data.getHigh() + "\n")
		  .append("                nLow = " + data.getLow() + "\n")
		  .append("              nMatch = " + data.getMatch() + "\n")
		  .append("           nAskPrice = " + arrayToStr(data.getAskPrice()) + "\n")
		  .append("            nAskVol] = " + arrayToStr(data.getAskVol()) + "\n")
		  .append("           nBidPrice = " + arrayToStr(data.getBidPrice()) + "\n")
		  .append("            nBidVol] = " + arrayToStr(data.getBidVol()) + "\n")
		  .append("          nNumTrades = " + data.getNumTrades() + "\n")
		  .append("             iVolume = " + data.getVolume() + "\n")
		  .append("           iTurnover = " + data.getTurnover() + "\n")
		  .append("        nTotalBidVol = " + data.getTotalBidVol() + "\n")
		  .append("        nTotalAskVol = " + data.getTotalAskVol() + "\n")
		  .append("nWeightedAvgBidPrice = " + data.getWeightedAvgBidPrice() + "\n")
		  .append("nWeightedAvgAskPrice = " + data.getWeightedAvgAskPrice() + "\n")
		  .append("               nIOPV = " + data.getIOPV() + "\n")
		  .append("    nYieldToMaturity = " + data.getYieldToMaturity() + "\n")
		  .append("        nHighLimited = " + data.getHighLimited() + "\n")
		  .append("         nLowLimited = " + data.getLowLimited() + "\n")
		  .append("            chPrefix = " + data.getPrefix() + "\n")
		  .append("               nSyl1 = " + data.getSyl1() + "\n")
		  .append("               nSyl2 = " + data.getSyl2() + "\n")
		  .append("                nSD2 = " + data.getSD2() + "\n")
		  .append("           TradeFlag = " + data.getTradeFlag() + "\n")
		  .append("          AfterPrice = " + data.getAfterPrice() + "\n")
		  .append("         AfterVolume = " + data.getAfterVolume() + "\n")
		  .append("       AfterTurnover = " + data.getAfterTurnover() + "\n")
		  .append("     AfterMatchItems = " + data.getAfterMatchItems() + "\n");
		
		System.out.println(sb.toString());
	}
	
	/*************************************Index Data***************************************/
	static void OutputIndexData(TDF_INDEX_DATA data, int serverTime, String localCurrentTime){  
/*
 "日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
 "交易日","状态"，"今开盘指数","最高指数","最低指数","最新指数","成交总量","成交总金额","前盘指数"
 * */	
        String[] contents = {CurrrentDate,
	             localCurrentTime,
	             String.valueOf(serverTime),
	             String.valueOf(data.getTime()),
	             data.getWindCode(),
	             data.getCode(),
	             String.valueOf(data.getActionDay()),
	             String.valueOf(data.getTradingDay()),   
	             String.valueOf(data.getStatus()),
	             String.valueOf(data.getOpenIndex()),
	             String.valueOf(data.getHighIndex()),
	             String.valueOf(data.getLowIndex()),
	             String.valueOf(data.getLastIndex()),
	             String.valueOf(data.getTotalVolume()),
	             String.valueOf(data.getTurnover()),
	             String.valueOf(data.getPreCloseIndex())	                                 
        };         
        wdhIndexData.addRecord(contents);		
	}
	
	static void printIndexData(TDF_INDEX_DATA data){
		System.out.println("TDF_INDEX_DATA");
		StringBuilder sb = new StringBuilder();
		sb.append("          SzWindCode = " + data.getWindCode() + "\n")
		  .append("              szCode = " + data.getCode() + "\n")
		  .append("          nActionDay = " + data.getActionDay() + "\n")
		  .append("         nTradingDay = " + data.getTradingDay() + "\n")
		  .append("               nTime = " + data.getTime() + "\n")
		  .append("             nStatus = " + data.getStatus() + "\n")
		  .append("          nOpenIndex = " + data.getOpenIndex() + "\n")
		  .append("          nHighIndex = " + data.getHighIndex() + "\n")
		  .append("           nLowIndex = " + data.getLowIndex() + "\n")
		  .append("          nLastIndex = " + data.getLastIndex() + "\n")
		  .append("        iTotalVolume = " + data.getTotalVolume() + "\n")
		  .append("      nPreCloseIndex = " + data.getPreCloseIndex() + "\n");
		System.out.println(sb.toString());
	}
	
	/*************************************Future Data***************************************/
	static void OutputFutureData(TDF_FUTURE_DATA data, int serverTime, String localCurrentTime, Demo demo){  
/*
    //期货行情		    
   {"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
    		            "交易日","状态","昨持仓","昨收盘价","昨结算","开盘价","最高价","最低价","最新价",
    		            "成交总量","成交总金额","持仓总量","今收盘","今结算","涨停价","跌停价","昨虚实度",
    		            "今虚实度","申卖价","申卖量","申买价","申买量"};
    //期权		    
    {"日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
    		            "交易日","状态","昨持仓","昨收盘价","昨结算","开盘价","最高价","最低价","最新价",
    		            "成交总量","成交总金额","持仓总量","今收盘","今结算","涨停价","跌停价","昨虚实度",
    		            "今虚实度","申卖价","申卖量","申买价","申买量","期权合约代码","标的证券代码",
    		            "认购认沽","行权日"};
 * */	
		//TDF_OPTION_CODE toc = demo.getOptionCodeInfo(data.getWindCode());
		//data.getC
		if (data.getCodeInfo() != null && (data.getCodeInfo().getType()&0xf0) == 0x90 ){ //期权	 
	        String[] contents = {CurrrentDate,
		             localCurrentTime,
		             String.valueOf(serverTime),
		             String.valueOf(data.getTime()),
		             data.getWindCode(),
		             data.getCode(),
		             String.valueOf(data.getActionDay()),
		             String.valueOf(data.getTradingDay()),        			             
		             String.valueOf(data.getStatus()),
		             String.valueOf(data.getPreOpenInterest()),
		             String.valueOf(data.getPreClose()),
		             String.valueOf(data.getPreSettlePrice()),
		             String.valueOf(data.getOpen()),
		             String.valueOf(data.getHigh()),
		             String.valueOf(data.getLow()),
		             String.valueOf(data.getMatch()),
		             String.valueOf(data.getVolume()),
		             String.valueOf(data.getTurnover()),
		             String.valueOf(data.getOpenInterest()),
		             String.valueOf(data.getClose()),
		             String.valueOf(data.getSettlePrice()),
		             String.valueOf(data.getHighLimited()),
		             String.valueOf(data.getLowLimited()),
		             String.valueOf(data.getPreDelta()),
		             String.valueOf(data.getCurrDelta()),	             
		             arrayToStr(data.getAskPrice()),
		             arrayToStr(data.getAskVol()),
		             arrayToStr(data.getBidPrice()),
		             arrayToStr(data.getBidVol()),
		             data.getCodeInfo().getExCodeInfo().getContractID(),
		             data.getCodeInfo().getExCodeInfo().getUnderlyingSecurityID(),
	                 String.valueOf(data.getCodeInfo().getExCodeInfo().getCallOrPut()),
	                 String.valueOf(data.getCodeInfo().getExCodeInfo().getExerciseDate())
	        };
			//contents[29] = 
			//contents[30] = data.getCodeInfo().getExCodeInfo().getUnderlyingSecurityID();
			//contents[31] = String.valueOf(data.getCodeInfo().getExCodeInfo().getCallOrPut());
			//contents[32] = String.valueOf(data.getCodeInfo().getExCodeInfo().getExerciseDate());
	        wdhOptionData.addRecord(contents);	
		}else{ //期货
	        String[] contents = {CurrrentDate,
		             localCurrentTime,
		             String.valueOf(serverTime),
		             String.valueOf(data.getTime()),
		             data.getWindCode(),
		             data.getCode(),
		             String.valueOf(data.getActionDay()),
		             String.valueOf(data.getTradingDay()),        			             
		             String.valueOf(data.getStatus()),
		             String.valueOf(data.getPreOpenInterest()),
		             String.valueOf(data.getPreClose()),
		             String.valueOf(data.getPreSettlePrice()),
		             String.valueOf(data.getOpen()),
		             String.valueOf(data.getHigh()),
		             String.valueOf(data.getLow()),
		             String.valueOf(data.getMatch()),
		             String.valueOf(data.getVolume()),
		             String.valueOf(data.getTurnover()),
		             String.valueOf(data.getOpenInterest()),
		             String.valueOf(data.getClose()),
		             String.valueOf(data.getSettlePrice()),
		             String.valueOf(data.getHighLimited()),
		             String.valueOf(data.getLowLimited()),
		             String.valueOf(data.getPreDelta()),
		             String.valueOf(data.getCurrDelta()),	             
		             arrayToStr(data.getAskPrice()),
		             arrayToStr(data.getAskVol()),
		             arrayToStr(data.getBidPrice()),
		             arrayToStr(data.getBidVol()),
		             data.getOriginalCode()                   
	        };	        
	        wdhFutureData.addRecord(contents);			
		}
	
	}	
	static void printFutureData(TDF_FUTURE_DATA data) {
		System.out.println("TDF_FUTURE_DATA");
		StringBuilder sb = new StringBuilder();
		sb.append("      szWindCode = " +  data.getWindCode() + "\n")
		  .append("          szCode = " +  data.getCode() + "\n")
		  .append("      nActionDay = " +  data.getActionDay() + "\n")
		  .append("     nTradingDay = " +  data.getTradingDay() + "\n")
		  .append("           nTime = " +  data.getTime() + "\n")
		  .append("         nStatus = " +  data.getStatus() + "\n")
		  .append("iPreOpenInterest = " +  data.getPreOpenInterest() + "\n")
		  .append("       nPreClose = " +  data.getPreClose() + "\n")
		  .append(" nPreSettlePrice = " +  data.getPreSettlePrice() + "\n")
		  .append("           nOpen = " +  data.getOpen() + "\n")
		  .append("           nHigh = " +  data.getHigh() + "\n")
		  .append("            nLow = " +  data.getLow() + "\n")
		  .append("          nMatch = " +  data.getMatch() + "\n")
		  .append("         iVolume = " +  data.getVolume() + "\n")
		  .append("       iTurnover = " +  data.getTurnover() + "\n")
		  .append("   iOpenInterest = " +  data.getOpenInterest() + "\n")
		  .append("          nClose = " +  data.getClose() + "\n")
		  .append("    nSettlePrice = " +  data.getSettlePrice() + "\n")
		  .append("    nHighLimited = " +  data.getHighLimited() + "\n")
		  .append("     nLowLimited = " +  data.getLowLimited() + "\n")
		  .append("       nPreDelta = " +  data.getPreDelta() + "\n")
		  .append("      nCurrDelta = " +  data.getCurrDelta() + "\n")
		  .append("       nAskPrice = " +  arrayToStr(data.getAskPrice()) + "\n")
		  .append("         nAskVol = " +  arrayToStr(data.getAskVol()) + "\n")
		  .append("       nBidPrice = " +  arrayToStr(data.getBidPrice()) + "\n")
		  .append("         nBidVol = " +  arrayToStr(data.getBidVol()) + "\n");
		
		System.out.println(sb.toString());	
	}
	
	/*************************************Transaction Data***************************************/
	static void OutputTransaction(TDF_TRANSACTION data, int serverTime, String localCurrentTime){  
/*
 * "日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
		    		           "成交编号","成交价格","成交数量","成交金额","买卖方向","成交类别","成交代码",
		    		           "卖方委托序号","买方委托序号", "Channel ID","BizIndex"		
 */
        String[] contents = {CurrrentDate,
	             localCurrentTime,
	             String.valueOf(serverTime),
	             String.valueOf(data.getTime()),
	             data.getWindCode(),
	             data.getCode(),
	             String.valueOf(data.getActionDay()),
	             String.valueOf(data.getIndex()),        			             
	             String.valueOf(data.getPrice()),
	             String.valueOf(data.getVolume()),
	             String.valueOf(data.getTurnover()),
	             String.valueOf(data.getBSFlag()),
	             String.valueOf(data.getOrderKind()),
	             String.valueOf(data.getFunctionCode()),
	             String.valueOf(data.getAskOrder()),
	             String.valueOf(data.getBidOrder()),
	             String.valueOf(data.getChannel()),
	             String.valueOf(data.getBizIndex())
        };         
        wdhTransactionData.addRecord(contents);
	}		
	static void printTransaction(TDF_TRANSACTION data ){
		System.out.println("TDF_TRANSACTION");
		StringBuilder sb = new StringBuilder();
		sb.append("    szWindCode = " +  data.getWindCode() + "\n")
		  .append("        szCode = " +  data.getCode() + "\n")
		  .append("    nActionDay = " +  data.getActionDay() + "\n")
		  .append("         nTime = " +  data.getTime() + "\n")
		  .append("        nIndex = " +  data.getIndex() + "\n")
		  .append("        nPrice = " +  data.getPrice() + "\n")
		  .append("       nVolume = " +  data.getVolume() + "\n")
		  .append("     nTurnover = " +  data.getTurnover() + "\n")
		  .append("       nBSFlag = " +  data.getBSFlag() + "\n")
		  .append("   chOrderKind = " +  data.getOrderKind() + "\n")
		  .append("chFunctionCode = " +  data.getFunctionCode() + "\n")
		  .append("     nAskOrder = " +  data.getAskOrder() + "\n")
		  .append("     nBidOrder = " +  data.getBidOrder() + "\n")
		  .append("     nChannel = "  +  data.getChannel() + "\n")
		  .append("    nBizIndex = "  +  data.getBizIndex() + "\n")
		  ;
		
		System.out.println(sb.toString());
	}
	
	/*************************************Order Data***************************************/
	static void OutputOrder(TDF_ORDER data, int serverTime, String localCurrentTime){  
/*
 * "日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
		    		           "委托号", "委托价格", "委托数量", "委托类别", "委托代码", "Channel ID",
		    		           "原始订单号", "业务编号"
 * */
        String[] contents = {CurrrentDate,
	             localCurrentTime,
	             String.valueOf(serverTime),
	             String.valueOf(data.getTime()),
	             data.getWindCode(),
	             data.getCode(),
	             String.valueOf(data.getActionDay()),
	             String.valueOf(data.getOrder()),        			             
	             String.valueOf(data.getPrice()),
	             String.valueOf(data.getVolume()),
	             String.valueOf(data.getOrderKind()),
	             String.valueOf(data.getFunctionCode()),
	    	     String.valueOf(data.getChannel()),
	    	     String.valueOf(data.getOrderOriNo()),
	    	     String.valueOf(data.getBizIndex())
        };         
        wdhOrderData.addRecord(contents);		
	}
	static void printOrder(TDF_ORDER data ){
		System.out.println("TDF_ORDER");
		StringBuilder sb = new StringBuilder();
		sb.append("    szWindCode = " +  data.getWindCode() + "\n")
		  .append("        szCode = " +  data.getCode() + "\n")
		  .append("    nActionDay = " +  data.getActionDay() + "\n")
		  .append("         nTime = " +  data.getTime() + "\n")
		  .append("        nOrder = " +  data.getOrder() + "\n")
		  .append("        nPrice = " +  data.getPrice() + "\n")
		  .append("       nVolume = " +  data.getVolume() + "\n")
		  .append("   chOrderKind = " +  data.getOrderKind() + "\n")
		  .append("chFunctionCode = " +  data.getFunctionCode() + "\n")
		  .append("     nChannel = "  +  data.getChannel() + "\n")
		  .append("   nOrderOriNo = " +  data.getOrderOriNo() + "\n")
		  .append("    nBizIndex = "  +  data.getBizIndex() + "\n")
		  ;

		System.out.println(sb.toString());
	}
	
	/*************************************OrderQueue Data***************************************/
	static void OutputOrderQueue(TDF_ORDER_QUEUE data, int serverTime, String localCurrentTime){  
		/*
		 * "日期","本地时间","服务器时间","交易所时间","万得代码","原始代码","业务发生日(自然日)",
		    		           "买卖方向","成交价格","订单数量","明细个数","订单明细"
		 * */
        String[] contents = {CurrrentDate,
	             localCurrentTime,
	             String.valueOf(serverTime),
	             String.valueOf(data.getTime()),
	             data.getWindCode(),
	             data.getCode(),
	             String.valueOf(data.getActionDay()),
	             String.valueOf(data.getSide()),        			             
	             String.valueOf(data.getPrice()),
	             String.valueOf(data.getOrders()),
	             String.valueOf(data.getABItems()),
	             arrayToStr(data.getABVolume(),data.getABItems())                   
        };         
        wdhOrderQueueData.addRecord(contents);		
	}
	static void printOrderQueue(TDF_ORDER_QUEUE data ){
		System.out.println("TDF_ORDER_QUEUE");
		StringBuilder sb = new StringBuilder();
		sb.append("     szWindCode = " +  data.getWindCode() + "\n")
		  .append("         szCode = " +  data.getCode() + "\n")
		  .append("     nActionDay = " +  data.getActionDay() + "\n")
		  .append("          nTime = " +  data.getTime() + "\n")
		  .append("          nSide = " +  data.getSide() + "\n")
		  .append("         nPrice = " +  data.getPrice() + "\n")
		  .append("        nOrders = " +  data.getOrders() + "\n")
		  .append("       nABItems = " +  data.getABItems() + "\n")
		  .append("      nABVolume = " +  arrayToStr(data.getABVolume(), data.getABItems()) + "\n");

		System.out.println(sb.toString());
	}

	/*************************************BbqTransaction Data**********************************/
	static void OutputBbqTransaction(TDF_BBQTRANSACTION data, int serverTime,String localCurrentTime) {
		/*
		 * "日期","本地时间","服务器时间","交易所时间","万得代码","业务发生日(自然日)","成交报价编号",
		 *  "成交价格/收益率","收益率/价格标识","删除/成交","成交方向","报价机构","行权/到期"
		 */
		String[] contents = { CurrrentDate, localCurrentTime,
				String.valueOf(serverTime), 
				String.valueOf(data.getTime()),
				data.getWindCode(),
				String.valueOf(data.getActionDay()),
				String.valueOf(data.getDoneID()),
				String.valueOf(data.getPrice()),
				String.valueOf(data.getPriceStatus()),
				String.valueOf(data.getStatus()),
				String.valueOf(data.getDirection()),
				String.valueOf(data.getSource()),
				String.valueOf(data.getSpecialFlag()),
				};
		wdhBbqTransaction.addRecord(contents);
	}

	static void printBbqTransaction(TDF_BBQTRANSACTION data) {
		System.out.println("TDF_BBQTRANSACTION");
		StringBuilder sb = new StringBuilder();
		sb.append("    					szWindCode = " + data.getWindCode() + "\n")
				.append("        		nActionDay = " + data.getActionDay() + "\n")
				.append("    				 nTime = " + data.getTime() + "\n")
				.append("         	   	   nDoneID = " + data.getDoneID() + "\n")
				.append("        			nPrice = " + data.getPrice() + "\n")
				.append("        	 chPriceStatus = " + data.getPriceStatus() + "\n")
				.append("       		  chStatus = " + data.getStatus() + "\n")
				.append("     		   chDirection = " + data.getDirection() + "\n")
				.append("       		  chSource = " + data.getSource() + "\n")
				.append("   		 chSpecialFlag = " + data.getSpecialFlag() + "\n");
		System.out.println(sb.toString());
	}

	/*************************************BbqBid Data*******************************************/
	static void OutputBbqBid(TDF_BBQBID_DATA data, int serverTime,String localCurrentTime) {
		/*
		 * "日期","本地时间","服务器时间","报价时间","万得代码","报价日(自然日)","报价机构",
		 * "买入收益率/价格","买入量","买入收益率/价格标识","是否bid","行权/到期","买入最优报价状态",
		 *  "卖出收益率/价格","卖出量","卖出收益率/价格标识","是否ofr","行权/到期","卖出最优报价状态"
		 */	  
		String[] contents = { CurrrentDate, localCurrentTime,
				String.valueOf(serverTime), String.valueOf(data.getTime()),
				data.getWindCode(),
				String.valueOf(data.getActionDay()),
				String.valueOf(data.getSource()),
				String.valueOf(data.getBidPrice()),
				String.valueOf(data.getBidVolume()), 
				String.valueOf(data.getBidPriceStatus()),
				String.valueOf(data.getIsBid()), 
				String.valueOf(data.getBidSpecialFlag()),
				String.valueOf(data.getBidStatus()),
				String.valueOf(data.getOfrPrice()),
				String.valueOf(data.getOfrVolume()),
				String.valueOf(data.getOfrPriceStatus()),
				String.valueOf(data.getIsOfr()),
				String.valueOf(data.getOfrSpecialFlag()),
				String.valueOf(data.getOfrStatus()),
				String.valueOf(data.getDoneID())
		  };
		// wdhMarketData.WriteRecordToFile(contents);
		wdhBbqBid.addRecord(contents);  
	}
	
	static void printBbqBid(TDF_BBQBID_DATA data) {
		System.out.println("TDF_BBQBID_DATA");
		StringBuilder sb = new StringBuilder();
		sb.append("          		SzWindCode = " + data.getWindCode() + "\n")
				.append("        	nActionDay = " + data.getActionDay() + "\n")
				.append("         		 nTime = " + data.getTime() + "\n")
				.append("             chSource = " + data.getSource() + "\n")
				.append("            nBidPrice = " + data.getBidPrice() + "\n")
				.append("           nBidVolume = " + data.getBidVolume() + "\n")
				.append("     chBidPriceStatus = " + data.getBidPriceStatus() + "\n")
				.append("              chIsBid = " + data.getIsBid() + "\n")
				.append("     chBidSpecialFlag = " + data.getBidSpecialFlag() + "\n")
				.append("          chBidStatus = " + data.getBidStatus() + "\n")
				.append("          	 nOfrPrice = " + data.getOfrPrice() + "\n")
				.append("           nOfrVolume = " + data.getOfrVolume() + "\n")
				.append("     chOfrPriceStatus = " + data.getOfrPriceStatus() + "\n")
				.append("     		   chIsOfr = " + data.getIsOfr()	+ "\n")
				.append("     chOfrSpecialFlag = " + data.getOfrSpecialFlag()	+ "\n")
				.append("          chOfrStatus = " + data.getOfrStatus() + "\n")
				.append("          	   nDoneID = " + data.getDoneID() + "\n");
		System.out.println(sb.toString());
	}
	
	/*************************************BrokerQueue Data***************************************/
	static void OutputBrokerQueue(TDF_BROKER_QUEUE data, int serverTime, String localCurrentTime){  
		/*
		 * "日期","本地时间","服务器时间","万得代码","原始代码","业务发生日(自然日)",
		   "叫卖时间","叫买时间","叫卖经纪个数","叫买经纪个数","叫卖前40经纪","叫买前40经纪"
		 * */
        String[] contents = {CurrrentDate,
	             localCurrentTime,
	             String.valueOf(serverTime),
	             data.getWindCode(),
	             data.getCode(),
	             String.valueOf(data.getActionDay()),
	             String.valueOf(data.getAskTime()),        			             
	             String.valueOf(data.getBidTime()),
	             String.valueOf(data.getAskBrokers()),
	             String.valueOf(data.getBidBrokers()),
	             arrayToStr(data.getAskBroker(),data.getAskBrokers()),
	             arrayToStr(data.getBidBroker(),data.getBidBrokers())   
        };         
        wdhBrokerQueue.addRecord(contents);		
	}
	static void printBrokerQueue(TDF_BROKER_QUEUE data ){
		System.out.println("TDF_BROKER_QUEUE");
		StringBuilder sb = new StringBuilder();
		sb.append("     szWindCode = " +  data.getWindCode() + "\n")
		  .append("         szCode = " +  data.getCode() + "\n")
		  .append("     nActionDay = " +  data.getActionDay() + "\n")
		  .append("        AskTime = " +  data.getAskTime() + "\n")
		  .append("        BidTime = " +  data.getBidTime() + "\n")
		  .append("     AskBrokers = " +  data.getAskBrokers() + "\n")
		  .append("     BidBrokers = " +  data.getBidBrokers() + "\n")
		  .append("      AskBroker = " +  arrayToStr(data.getAskBroker(), data.getAskBrokers()) + "\n")
		  .append("      BidBroker = " +  arrayToStr(data.getBidBroker(), data.getBidBrokers()) + "\n");

		System.out.println(sb.toString());
	}
	
	
	/*************************************SYS Data***************************************/
	static void printConnectResult(TDF_CONNECT_RESULT data ){
		System.out.println("TDF_CONNECT_RESULT");
		StringBuilder sb = new StringBuilder();
		sb.append("            szIp = " +  data.getIp() + "\n")
		  .append("          szPort = " +  data.getPort() + "\n")
		  .append("          szUser = " +  data.getUser() + "\n")
		  .append("           szPwd = " +  data.getPwd() + "\n")
		  .append("   nConnectionID = " +  data.getConnectionID() + "\n")
		  .append("     nConnResult = " +  data.getConnResult() + "\n");

		System.out.println(sb.toString());
	}

	static void printLoginResult(TDF_LOGIN_RESULT data ){
		System.out.println("登陆结果：");
		StringBuilder sb = new StringBuilder();
		sb.append("    nLoginResult = " +  data.getLoginResult() + "\n")
		  .append("          szInfo = " +  data.getInfo() + "\n")
		  .append("        nMarkets = " +  data.getMarkets() + "\n")
		  .append("        szMarket = " +  arrayToStr(data.getMarket(), data.getMarkets()) + "\n")
		  .append("        nDynDate = " +  arrayToStr(data.getDynDate(), data.getMarkets()) + "\n");

		System.out.println(sb.toString());
	}

	static void printCodeTableResult(TDF_CODE_RESULT data ){
		System.out.println("TDF_CODE_RESULT");
		StringBuilder sb = new StringBuilder();
		sb.append("          szInfo = " +  data.getInfo() + "\n")
		  .append("        nMarkets = " +  data.getMarkets() + "\n")
		  .append("        szMarket = " +  arrayToStr(data.getMarket(), data.getMarkets()) + "\n")
		  .append("      nCodeCount = " +  arrayToStr(data.getCodeCount(), data.getMarkets()) + "\n")
		  .append("       nCodeDate = " +  arrayToStr(data.getCodeDate(), data.getMarkets()) + "\n");

		System.out.println(sb.toString());
	}

	static void printAddCode(TDF_ADD_CODE addCode) {
		System.out.println("TDF_ADD_CODE");
		StringBuilder sb = new StringBuilder();
		sb.append("          Market = " +  addCode.getMarket() + "\n")
		  .append("        CodeDate = " +  addCode.getCodeDate() + "\n")
		  .append("        CodeCout = " +  addCode.getCodes().length + "\n");

		System.out.println(sb.toString());
	}
	
	static void printOptionCode(TDF_OPTION_CODE data)
	{
		System.out.println("TDF_OPTION_CODE");
		StringBuilder sb = new StringBuilder();
		sb.append("        szContractID = " +  data.getContractID() + "\n")
		  .append("        szUnderlyingSecurityID = " +  data.getUnderlyingSecurityID() + "\n")
		  .append("        chCallOrPut = " +  data.getCallOrPut() + "\n")
		  .append("        szWindCode = " +  data.getBasicCode().getWindCode() + "\n")
		  .append("        szMarket = " +  data.getBasicCode().getMarket() + "\n")
		  .append("        szCNName = " +  data.getBasicCode().getCNName() + "\n")
		  .append("        ContractID = " +  data.getContractID() + "\n")
		  .append("        UnderlyingSecurityID = " +  data.getUnderlyingSecurityID() + "\n")
		  .append("        CallOrPut = " +  data.getCallOrPut() + "\n")
		  .append("        ExerciseDate = " +  data.getExerciseDate() + "\n")
		  .append("        OptionType = " +  data.getOptionType() + "\n");

		System.out.println(sb.toString());
	}

}

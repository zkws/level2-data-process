package com.alphainv.tdfapi;

public class TransactionDataInfo {

    private String exchangeTime;
    private String stockCode;
    private Character stkFunctionCode;
    private Long turnOver;
    private Integer volumeNum;
    private Integer channelId;
    // 卖方委托序号
    private Integer askOrderNum;
    // 买方委托序号
    private Integer bidOrderNum;
    private Integer BSFlag;

    TransactionDataInfo(String exchangeTime, String stockCode, Character stkFunctionCode, Long turnOver, Integer volumeNum, Integer channelId, Integer askOrderNum, Integer bidOrderNum, Integer BSFlag){
        this.stkFunctionCode = stkFunctionCode;
        this.turnOver = turnOver;
        this.volumeNum = volumeNum;
        this.channelId = channelId;
        this.exchangeTime = exchangeTime;
        this.stockCode = stockCode;
        this.askOrderNum = askOrderNum;
        this.bidOrderNum = bidOrderNum;
        this.BSFlag = BSFlag;
    }

    public String getExchangeTime() {
        return exchangeTime;
    }

    public String getStockCode() {
        return stockCode;
    }

    public Character getStkFunctionCode() {
        return stkFunctionCode;
    }

    public Long getTurnOver() {
        return turnOver;
    }

    public Integer getVolumeNum() {
        return volumeNum;
    }

    public Integer getChannelId() {
        return channelId;
    }

    public Integer getAskOrderNum() {
        return askOrderNum;
    }

    public Integer getBidOrderNum() {
        return bidOrderNum;
    }

    public Integer getBSFlag() {
        return BSFlag;
    }
}

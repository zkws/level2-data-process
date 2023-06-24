package com.alphainv.tdfapi;

public class MarketDataInfo {
    private Long highLimit;
    private Long lowLimit;
    private Long recentPrice;
    private Long preClose;
    private String exchangeTime;
    private String stockCode;

    MarketDataInfo(Long highLimit, Long lowLimit, Long recentPrice, Long preClose, String exchangeTime, String stockCode){
        this.highLimit = highLimit;
        this.lowLimit = lowLimit;
        this.recentPrice = recentPrice;
        this.preClose = preClose;
        this.exchangeTime = exchangeTime;
        this.stockCode = stockCode;
    }

    public Long getHighLimit() {
        return highLimit;
    }

    public Long getLowLimit() {
        return lowLimit;
    }

    public Long getRecentPrice() {
        return recentPrice;
    }

    public Long getPreClose() {
        return preClose;
    }

    public String getExchangeTime() {
        return exchangeTime;
    }

    public String getStockCode() {
        return stockCode;
    }
}

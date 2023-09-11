package com.alphainv.tdfapi;

public class IndexDataInfo {
    private Long nHighIndex;
    private Long nLowIndex;
    private Long nOpenIndex;
    private Long nLastIndex;
    private Long nPreCloseIndex;
    private String exchangeTime;
    private String indexCode;

    public IndexDataInfo(Long nHighIndex, Long nLowIndex, Long nOpenIndex, Long nLastIndex, Long nPreCloseIndex, String exchangeTime, String indexCode) {
        this.nHighIndex = nHighIndex;
        this.nLowIndex = nLowIndex;
        this.nOpenIndex = nOpenIndex;
        this.nLastIndex = nLastIndex;
        this.nPreCloseIndex = nPreCloseIndex;
        this.exchangeTime = exchangeTime;
        this.indexCode = indexCode;
    }

    public Long getnHighIndex() {
        return nHighIndex;
    }

    public Long getnLowIndex() {
        return nLowIndex;
    }

    public Long getnOpenIndex() {
        return nOpenIndex;
    }

    public Long getnLastIndex() {
        return nLastIndex;
    }

    public Long getnPreCloseIndex() {
        return nPreCloseIndex;
    }

    public String getExchangeTime() {
        return exchangeTime;
    }

    public String getIndexCode() {
        return indexCode;
    }

    public void setnHighIndex(Long nHighIndex) {
        this.nHighIndex = nHighIndex;
    }

    public void setnLowIndex(Long nLowIndex) {
        this.nLowIndex = nLowIndex;
    }

    public void setnOpenIndex(Long nOpenIndex) {
        this.nOpenIndex = nOpenIndex;
    }

    public void setnLastIndex(Long nLastIndex) {
        this.nLastIndex = nLastIndex;
    }

    public void setnPreCloseIndex(Long nPreCloseIndex) {
        this.nPreCloseIndex = nPreCloseIndex;
    }

    public void setExchangeTime(String exchangeTime) {
        this.exchangeTime = exchangeTime;
    }

    public void setIndexCode(String indexCode) {
        this.indexCode = indexCode;
    }
}

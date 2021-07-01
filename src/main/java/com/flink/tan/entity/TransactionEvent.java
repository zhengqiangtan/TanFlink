package com.flink.tan.entity;


/**
 *  account 表是账户信息，amount 为转账金额，timeStamp 是交易时的时间戳信息
 */
public class TransactionEvent {


    private String accout;
    private Double amount;
    private Long timeStamp;

    public String getAccout() {
        return accout;
    }

    public void setAccout(String accout) {
        this.accout = accout;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public TransactionEvent(String accout, Double amount, Long timeStamp) {
        this.accout = accout;
        this.amount = amount;
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "accout='" + accout + '\'' +
                ", amount=" + amount +
                ", timeStamp=" + timeStamp +
                '}';
    }
}

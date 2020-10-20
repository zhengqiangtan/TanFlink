package com.project.tan.entity;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/20 1:59 PM
 * @Version 1.0
 */
public class PageVisit {
    public String visitTime;
    public long userId;
    public String visitPage;

    // public constructor to make it a Flink POJO
    public PageVisit() {
    }

    public PageVisit(String visitTime, long userId, String visitPage) {
        this.visitTime = visitTime;
        this.userId = userId;
        this.visitPage = visitPage;
    }

    @Override
    public String toString() {
        return "PageVisit " + visitTime + " " + userId + " " + visitPage;
    }
}

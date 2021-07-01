package com.flink.tan.entity;

/**
 * @Author zhengqiang.tan
 * @Date 2020/10/25 10:48 PM
 * @Version 1.0
 */
public class AlertEvent {

    private String id;

    private String message;

    public String getId() {

        return id;

    }

    public void setId(String id) {

        this.id = id;

    }

    public String getMessage() {

        return message;

    }

    public void setMessage(String message) {

        this.message = message;

    }

    public AlertEvent(String id, String message) {

        this.id = id;

        this.message = message;

    }

    @Override

    public String toString() {

        return "AlertEvent{" +

                "id='" + id + '\'' +

                ", message='" + message + '\'' +

                '}';

    }

}

package com.sender.sender.model;

public class SQSMessage {
    private String msg;

    public SQSMessage(){}

    public SQSMessage(String msg) {
        this.msg = msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
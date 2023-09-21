package com.sender.sender.model;

public class S3Message {


    private String msg;

    public S3Message(){}

    public S3Message(String msg) {
        this.msg = msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }
}
package com.xxx.demons.roketmessage;

import com.google.gson.Gson;

import com.xxx.demons.roketmessage.util.RocketUtil;

import org.apache.rocketmq.client.producer.DefaultMQProducer;

import io.vertx.core.Vertx;

public class Message<T> {
    boolean failed;
    int code;
    String errorMessage;
    String seq;
    private String replyAddress;
    private T sentBody;
    private T receivedBody;
    private DefaultMQProducer producer;
    private Gson GSON;
    Vertx vertx;

    public T body() {
        if (receivedBody == null && sentBody != null) {
            receivedBody = sentBody;
        }
        return receivedBody;
    }

    public void reply(T msg) {
        Message message = new Message();
        message.seq = seq;
        message.replyAddress = replyAddress;
        message.receivedBody = msg;
        RocketUtil.sendReplyMessage(producer, replyAddress, GSON.toJson(message));
    }

    public void fail(int code, String errorMessage) {
        Message message = new Message();
        message.failed = true;
        message.seq = seq;
        message.replyAddress = replyAddress;
        message.code = code;
        message.errorMessage = errorMessage;
        RocketUtil.sendReplyMessage(producer, replyAddress, GSON.toJson(message));
    }

    public void wrapReceived(DefaultMQProducer producer, Gson GSON) {
        this.producer = producer;
        this.GSON = GSON;
    }

    public static <T> Message<T> wrapSend(String seq, String replyAddress, T sentBody) {
        Message<T> message = new Message<>();
        message.seq = seq;
        message.replyAddress = replyAddress;
        message.sentBody = sentBody;
        return message;
    }
}

package com.xxx.demons.kafkamessage;

import com.google.gson.Gson;

import com.xxx.demons.kafkamessage.util.KafkaUtil;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;

public class Message<T> {
    boolean failed;
    int code;
    String errorMessage;
    String seq;
    private String replyAddress;
    private T sentBody;
    private T receivedBody;
    private KafkaProducer<String, String> producer;
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
        KafkaUtil.sendReplyMessage(vertx, producer, replyAddress, GSON.toJson(message));
    }

    public void fail(int code, String errorMessage) {
        Message message = new Message();
        message.failed = true;
        message.seq = seq;
        message.replyAddress = replyAddress;
        message.code = code;
        message.errorMessage = errorMessage;
        KafkaUtil.sendReplyMessage(vertx, producer, replyAddress, GSON.toJson(message));
    }

    public void wrapReceived(KafkaProducer<String, String> producer, Gson GSON) {
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

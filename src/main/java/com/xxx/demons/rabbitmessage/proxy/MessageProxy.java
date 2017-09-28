package com.xxx.demons.rabbitmessage.proxy;

import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;

public class MessageProxy implements Message<String> {
    private String body;
    private Consumer<String> replyHandler;
    private BiConsumer<Integer, String> failHandler;


    public MessageProxy(String body) {
        this.body = body;
    }

    public MessageProxy(String body, Consumer<String> replyHandler, BiConsumer<Integer, String> failHandler) {
        this.body = body;
        this.replyHandler = replyHandler;
        this.failHandler = failHandler;
    }

    @Override
    public String address() {
        return null;
    }

    @Override
    public MultiMap headers() {
        return null;
    }

    @Override
    public String body() {
        return body;
    }

    @Override
    public String replyAddress() {
        return null;
    }

    @Override
    public boolean isSend() {
        return false;
    }

    @Override
    public void reply(Object message) {
        replyHandler.accept(message.toString());
    }

    @Override
    public void reply(Object message, DeliveryOptions options) {
        reply(message);
    }

    @Override
    public void fail(int failureCode, String message) {
        failHandler.accept(failureCode, message);
    }

    @Override
    public void reply(Object message, DeliveryOptions options, Handler replyHandler) {
        reply(message);
    }

    @Override
    public void reply(Object message, Handler replyHandler) {
        reply(message);
    }
}

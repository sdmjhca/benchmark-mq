package com.xxx.demons.netsmessage;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageProducer;

public class MessageProducerProxy implements MessageProducer {
    private String address;
    private EventBusProxy eventBus;

    public MessageProducerProxy(String address, EventBusProxy eventBus) {
        this.address = address;
        this.eventBus = eventBus;
    }

    @Override
    public MessageProducer send(Object message) {
        eventBus.send(address, message);
        return this;
    }

    @Override
    public MessageProducer write(Object data) {
        eventBus.publish(address, data);
        return this;
    }

    @Override
    public MessageProducer setWriteQueueMaxSize(int maxSize) {
        return null;
    }

    @Override
    public boolean writeQueueFull() {
        return false;
    }

    @Override
    public MessageProducer deliveryOptions(DeliveryOptions options) {
        return this;
    }

    @Override
    public String address() {
        return null;
    }

    @Override
    public void end() {

    }

    @Override
    public void close() {

    }

    @Override
    public MessageProducer drainHandler(Handler handler) {
        return this;
    }

    @Override
    public MessageProducer exceptionHandler(Handler handler) {
        return this;
    }

    @Override
    public MessageProducer send(Object message, Handler replyHandler) {
        eventBus.send(address, message, replyHandler);
        return this;
    }
}

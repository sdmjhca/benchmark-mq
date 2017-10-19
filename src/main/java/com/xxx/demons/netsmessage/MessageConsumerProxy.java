package com.xxx.demons.netsmessage;

import io.vertx.core.Handler;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.streams.ReadStream;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MessageConsumerProxy implements MessageConsumer {
    private Runnable unSubscribe;

    public MessageConsumerProxy(Runnable unSubscribe) {
        this.unSubscribe = unSubscribe;
    }

    @Override
    public MessageConsumer exceptionHandler(Handler handler) {
        return null;
    }

    @Override
    public MessageConsumer handler(Handler handler) {
        return null;
    }

    @Override
    public MessageConsumer pause() {
        return null;
    }

    @Override
    public MessageConsumer resume() {
        return null;
    }

    @Override
    public ReadStream bodyStream() {
        return null;
    }

    @Override
    public boolean isRegistered() {
        return false;
    }

    @Override
    public String address() {
        return null;
    }

    @Override
    public MessageConsumer setMaxBufferedMessages(int maxBufferedMessages) {
        return null;
    }

    @Override
    public int getMaxBufferedMessages() {
        return 0;
    }

    @Override
    public void unregister() {
        unSubscribe.run();
    }

    @Override
    public void unregister(Handler completionHandler) {
        unregister();
    }

    @Override
    public void completionHandler(Handler completionHandler) {

    }

    @Override
    public MessageConsumer endHandler(Handler endHandler) {
        return null;
    }
}

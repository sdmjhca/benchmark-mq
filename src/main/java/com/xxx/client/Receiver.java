package com.xxx.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Receiver extends AbstractVerticle {
    private final int index;
    public final String name;
    public String address;
    private Vertx vertx;

    private long lastPush;

    public Receiver(Vertx vertx, int index) {
        this.vertx = vertx;
        this.index = index;
        this.name = "Receiver" + index;
        this.address = buildAddress(index);
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(address, this::onMessage);
        vertx.eventBus().consumer(Pusher.ADDRESS, this::onPush);
        log.debug("receiver {} started", index);
    }

    private void onMessage(Message<String> message) {
//        log.debug("{} receive {}", name, message.body());
        Starter.receiverMeter.mark();
        message.reply("ok");
    }

    private void onPush(Message<String> message) {
        long push = Long.parseLong(message.body());
        if (push - lastPush != 1) log.debug("{} push lost {}->>{}", index, lastPush, push);
        lastPush = push;
        Starter.pushReceiveMeter.mark();
    }

    public static String buildAddress(int index) {
        return "/receiver1" + index;
    }
}

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


    public Receiver(Vertx vertx, int index) {
        this.vertx = vertx;
        this.index = index;
        this.name = "Receiver" + index;
        this.address = buildAddress(index);
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(address, this::onMessage);
    }

    public void onMessage(Message<String> message) {
//        log.debug("{} receive {}", name, message.body());
        Starter.receiverMeter.mark();
        message.reply("ok");
    }

    public static String buildAddress(int index) {
        return "/receiver1" + index;
    }
}

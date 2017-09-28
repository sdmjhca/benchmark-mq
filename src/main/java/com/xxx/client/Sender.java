package com.xxx.client;

import com.xxx.bootstrap.EntryPoint;

import java.util.Random;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Sender extends AbstractVerticle {
    private final int index;
    public final String name;
    private Vertx vertx;
    private final int[] times = {1};
    public static final String CMD_ADDRESS = "/cmd_sender";

    private int RECEIVER_COUNT = Integer.parseInt(EntryPoint.getEnv("RECEIVER_COUNT"));


    public Sender(Vertx vertx, int index) {
        this.vertx = vertx;
        this.index = index;
        this.name = "Sender" + index;
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(CMD_ADDRESS, this::onMessage);
    }

    public void onMessage(Message<String> msg) {
        long count = Long.parseLong(msg.body());
        for (int i = 0; i < count; i++) {
            String message = name + " send " + (times[0]++) + " times";
            Future<Message<String>> future = Future.future();
            Starter.senderMeter.mark();
            int random = new Random().nextInt(RECEIVER_COUNT);
            vertx.eventBus().send(Receiver.buildAddress(random), message, future.completer());
            future.setHandler(r -> {
                if (r.succeeded()) {
//                    log.debug("reply {}", r.result().body());
                    Starter.replyMeter.mark();
                } else {
                    log.error("", r.cause());
                }
            });
        }
    }
}

package com.xxx.client;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Pusher extends AbstractVerticle {
    public static final String ADDRESS = "/pusher";

    private Vertx vertx;

    private int pushCount = 1;

    public Pusher(Vertx vertx) {
        this.vertx = vertx;
    }

    @Override
    public void start() throws Exception {
        vertx.setPeriodic(500, l -> publish());
        log.debug("pusher started");
    }

    private void publish() {
        vertx.eventBus().publish(ADDRESS, "" + pushCount);
        Starter.pushMeter.mark();
        pushCount++;
    }
}

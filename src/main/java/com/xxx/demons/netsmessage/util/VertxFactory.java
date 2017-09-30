package com.xxx.demons.netsmessage.util;


import com.xxx.demons.netsmessage.ReceiverPoint;
import com.xxx.demons.netsmessage.VertxProxy;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.nats.client.Connection;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class VertxFactory {
    private Vertx vertx = Vertx.vertx();


    @Resource
    private Connection natsConnection;

    @Resource
    private ReceiverPoint receiverPoint;


    @PostConstruct
    public void init() throws Exception {
    }

    public Vertx createVertx() {
        return new VertxProxy(vertx, natsConnection, receiverPoint);
    }
}

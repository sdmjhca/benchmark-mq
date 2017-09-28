package com.xxx.demons.rabbitmessage.util;

import com.rabbitmq.client.Connection;
import com.xxx.demons.rabbitmessage.proxy.ReceiverPoint;
import com.xxx.demons.rabbitmessage.proxy.VertxProxy;

import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class VertxFactory {
    private Vertx vertx = Vertx.vertx();

    @Resource
    private Connection rabbitConnection;

    @Resource
    private ReceiverPoint receiverPoint;

    @PostConstruct
    public void init() throws Exception {
    }

    public Vertx createVertx() {
        return new VertxProxy(vertx, rabbitConnection, receiverPoint);
    }
}

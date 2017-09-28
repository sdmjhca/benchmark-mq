package com.xxx.client;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.xxx.util.LogHelper;

import org.apache.rocketmq.client.exception.MQClientException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class Starter extends AbstractVerticle {
    @Value("#{environment.SEND_SPEED}")
    private int SEND_SPEED;
    @Value("#{environment.DISABLE_SEND}")
    private String DISABLE_SEND;
    @Value("#{environment.PARALLELISM}")
    private int PARALLELISM;
    @Value("#{environment.MODE}")
    private String MODE;
    @Value("#{environment.RECEIVER_COUNT}")
    private int RECEIVER_COUNT;
    @Resource
    private Vertx vertx;

    public static MetricRegistry metrics = new MetricRegistry();
    public static Meter senderMeter = metrics.meter("sender");
    public static Meter receiverMeter = metrics.meter("receiver");
    public static Meter replyMeter = metrics.meter("reply");

    @PostConstruct
    public void initialize() throws MQClientException {
        if (MODE.equals("both") || MODE.equals("receiver")) {
            for (int i = 0; i < RECEIVER_COUNT; i++) {
                Receiver receiver = new Receiver(vertx, i);
                vertx.deployVerticle(receiver, r -> LogHelper.deploying(receiver.name, r, log));
            }
        }
        if (MODE.equals("both") || MODE.equals("sender")) {
            int count = SEND_SPEED / PARALLELISM;
            vertx.setTimer(3000, l -> {
                for (int i = 0; i < PARALLELISM; i++) {
                    Sender sender = new Sender(vertx, i);
                    vertx.deployVerticle(sender, r -> LogHelper.deploying(sender.name, r, log));
                }
                vertx.setPeriodic(1000, t -> vertx.eventBus().publish(Sender.CMD_ADDRESS, String.valueOf(count)));
            });
        }
        startReport();
    }

    private void startReport() {
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS).convertDurationsTo(TimeUnit.MILLISECONDS).build();
        reporter.start(1, TimeUnit.SECONDS);
    }
}

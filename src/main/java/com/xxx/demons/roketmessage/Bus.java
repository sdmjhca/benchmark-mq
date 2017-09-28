package com.xxx.demons.roketmessage;

import com.google.gson.Gson;

import com.xxx.demons.roketmessage.util.RocketUtil;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Bus<T> {
    private final String ID = UUID.randomUUID().toString();
    private final String NAME;
    private final String REPLY_ADDRESS;
    private final Map<String, Handler<AsyncResult<Message<T>>>> replyFutures = new HashMap<>();
    //    private final Map<String, Long> timeoutTimers = new HashMap<>();
    private final Gson GSON = new Gson();
    private DefaultMQProducer producer;
    private Vertx vertx;

    public Bus(Vertx vertx, String name) throws MQClientException {
        this.vertx = vertx;
        this.NAME = name;
        this.REPLY_ADDRESS = filterAddress(("reply-" + NAME));
        this.producer = new DefaultMQProducer("default");
        this.producer.setInstanceName(UUID.randomUUID().toString());
        this.producer.start();
        Context runContext = vertx.getOrCreateContext();
        buildConsumer(REPLY_ADDRESS, (msgs, context) -> {
//            System.out.printf(Thread.currentThread().getName() + " Receive New Messages: " + msgs + "%n");
            msgs.forEach(msg -> {
                String value = new String(msg.getBody());
                Message<T> message = GSON.fromJson(value, Message.class);
                message.vertx = vertx;
                log.debug("Bus {} reply seq {}", REPLY_ADDRESS, message.seq);
                replyFutures.computeIfPresent(message.seq, (seq, handler) -> {
                    if (message.failed) {
                        runContext.runOnContext(r ->
                                handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, message.code, message.errorMessage))));
                    } else {
                        runContext.runOnContext(r -> handler.handle(Future.succeededFuture(message)));
                    }
                    return null;
                });
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }

    private DefaultMQPushConsumer buildConsumer(String address, MessageListenerConcurrently listener) {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("default");
        consumer.setInstanceName(UUID.randomUUID().toString());
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
        consumer.setMessageModel(MessageModel.BROADCASTING);

        try {
            consumer.subscribe(address, "*");
            consumer.registerMessageListener(listener);
            consumer.start();
        } catch (MQClientException e) {
            log.error("", e);
            throw new RuntimeException(e.getErrorMessage());
        }
        return consumer;
    }

    public DefaultMQPushConsumer consumer(String address, Handler<Message<T>> handler) {
        String addr = filterAddress(address);
        log.debug("Bus {} consumer {}", REPLY_ADDRESS, addr);
        Context runContext = vertx.getOrCreateContext();
        return buildConsumer(addr, (msgs, context) -> {
            msgs.forEach(msg -> {
                String value = new String(msg.getBody());
                log.debug("Bus {} consumer {} receive {}", REPLY_ADDRESS, addr, value);
                Message<T> message = GSON.fromJson(value, Message.class);
                message.vertx = vertx;
                message.wrapReceived(producer, GSON);
                runContext.runOnContext(r -> handler.handle(message));
            });
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
    }

    public void send(String address, T message) {
        send(address, message, null);
    }

    public void send(String address, T message, Handler<AsyncResult<Message<T>>> handler) {
        address = filterAddress(address);
        Message wrap = Message.wrapSend(UUID.randomUUID().toString(), REPLY_ADDRESS, message);
        log.debug("Bus {} send {} to {}", REPLY_ADDRESS, message, address);
        if (handler != null) {
            replyFutures.put(wrap.seq, handler);
        }
        RocketUtil.sendMessage(producer, address, GSON.toJson(wrap));
    }

    public void publish(String address, T message) {
        address = filterAddress(address);
        Message wrap = Message.wrapSend(UUID.randomUUID().toString(), REPLY_ADDRESS, message);
        RocketUtil.sendMessage(producer, address, GSON.toJson(wrap));
    }

    public String filterAddress(String address) {
        return address.replaceAll("/", "-");
    }
}

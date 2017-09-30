package com.xxx.demons.netsmessage;


import java.io.IOException;
import java.nio.charset.Charset;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.nats.client.AsyncSubscription;
import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.eventbus.SendContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventBusProxy implements EventBus {
    private Vertx vertx;
    private Connection connection;
    private ReceiverPoint receiverPoint;

    public EventBusProxy(Vertx vertx, Connection connection, ReceiverPoint receiverPoint) {
        this.vertx = vertx;
        this.connection = connection;
        this.receiverPoint = receiverPoint;
    }


    public void reply(String replyTo, String replyMsg) {
        try {
            connection.publish(replyTo, replyMsg.getBytes());
            log.debug("eventbus reply msg {} and to {}", replyMsg, replyTo);
        } catch (IOException e) {
            log.error("error ", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public EventBus send(String address, Object message) {
        return sendCommon(address, message, null);
    }

    @Override
    public <T> EventBus send(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
        return sendCommon(address, message, replyHandler);
    }

    private <T> EventBus sendCommon(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
        Context runContext = vertx.getOrCreateContext();
        try {
            String corrId = UUID.randomUUID().toString();
            if (replyHandler != null) {
                Handler<AsyncResult<Message<T>>> receiveHandler = async -> {
                    runContext.runOnContext(r -> replyHandler.handle(async));
                };
                receiverPoint.putHandler(corrId, receiveHandler);
            }
            byte[] bytes = (corrId + "#" + message).getBytes();
            log.debug("eventbus send msg body {} msg size {} and should reply {}", new String((corrId + "#" + message).getBytes()), bytes.length,
                    receiverPoint.replyQueueName);
            connection.publish(address, receiverPoint.replyQueueName, (corrId + "#" + message).getBytes());
            return this;
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public EventBus send(String address, Object message, DeliveryOptions options) {
        return sendCommon(address, message, null);
    }

    @Override
    public <T> EventBus send(String address, Object message, DeliveryOptions options, Handler<AsyncResult<Message<T>>> replyHandler) {
        return sendCommon(address, message, replyHandler);
    }

    @Override
    public EventBus publish(String address, Object message) {
        return publishCommon(address, message);
    }

    @Override
    public EventBus publish(String address, Object message, DeliveryOptions options) {
        return publishCommon(address, message);
    }

    public EventBus publishCommon(String address, Object message) {
        try {
            String corrId = UUID.randomUUID().toString();
            connection.publish(address, receiverPoint.replyQueueName, (corrId + "#" + message).toString().getBytes(Charset
                    .defaultCharset()));
            log.debug("eventbus send msg {}", (corrId + "#" + message).toString());
            return this;
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public <T> MessageConsumer<T> consumer(String address) {
        return null;
    }

    @Override
    public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
        try {
            Context runContext = vertx.getOrCreateContext();

            AsyncSubscription subscribe = connection.subscribe(address, cb -> {
                String s = new String(cb.getData());
                log.debug("eventbus  consumer msg {} ", s);
                String[] split = s.split("#");
                Consumer<String> replyHandler = msg -> {
                    reply(cb.getReplyTo(), "0" + "#" + split[0] + "#" + msg);
                };
                BiConsumer<Integer, String> failedHandler = (code, msg) -> {
                    reply(cb.getReplyTo(), "1" + "#" + split[0] + "#" + code + "#" + msg);
                };
                Message message = new MessageProxy(split[1], replyHandler, failedHandler);
                runContext.runOnContext(r -> handler.handle(message));
            });

            return new MessageConsumerProxy(subscribe);
        } catch (Exception e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    @Override
    public <T> MessageConsumer<T> localConsumer(String address) {
        return null;
    }

    @Override
    public <T> MessageConsumer<T> localConsumer(String address, Handler<Message<T>> handler) {
        return null;
    }

    @Override
    public <T> MessageProducer<T> sender(String address) {
        return null;
    }

    @Override
    public <T> MessageProducer<T> sender(String address, DeliveryOptions options) {
        return null;
    }

    @Override
    public <T> MessageProducer<T> publisher(String address) {
        return new MessageProducerProxy(address, this);
    }

    @Override
    public <T> MessageProducer<T> publisher(String address, DeliveryOptions options) {
        return new MessageProducerProxy(address, this);
    }

    @Override
    public EventBus registerCodec(MessageCodec codec) {
        return null;
    }

    @Override
    public EventBus unregisterCodec(String name) {
        return null;
    }

    @Override
    public <T> EventBus registerDefaultCodec(Class<T> clazz, MessageCodec<T, ?> codec) {
        return null;
    }

    @Override
    public EventBus unregisterDefaultCodec(Class clazz) {
        return null;
    }

    @Override
    public void start(Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {

    }

    @Override
    public EventBus addInterceptor(Handler<SendContext> interceptor) {
        return null;
    }

    @Override
    public EventBus removeInterceptor(Handler<SendContext> interceptor) {
        return null;
    }

    @Override
    public boolean isMetricsEnabled() {
        return false;
    }
}

package com.xxx.demons.netsmessage;


import com.xxx.util.ConcurrentQueue;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
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
    private ReceiverPoint point;
    private static AtomicLong seqInc = new AtomicLong(1000);
    private final static Map<String, ConcurrentQueue<Void>> queueMap = new ConcurrentHashMap<>();
    private final static Map<String, Long> ackRecords = new ConcurrentHashMap<>();

    private <T> EventBus sendCommon(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
        Future<Void> trigger = Future.future();

        trigger.map(v -> {
            Context runContext = vertx.getOrCreateContext();
            long seqNum = seqInc.incrementAndGet();
            byte[] bytes = MsgCodec.encode(seqNum, seqNum, message.toString());
            Runnable retry = () -> {
                log.warn("event bus retry send {} msg seq {} body {} msg size {} and should reply {}", address, seqNum, message, bytes.length,
                        point.replyAddress);
                SendHelper.publish(connection, address, point.replyAddress, bytes);
            };
            if (replyHandler != null) {
                Handler<AsyncResult<Message<T>>> receiveHandler = async -> runContext.runOnContext(r -> replyHandler.handle(async));
                point.putHandler(seqNum, receiveHandler, retry, () -> queueMap.get(address).take());
            }
            log.debug("event bus send {} msg seq {} body {} msg size {} and should reply {}", address, seqNum, message, bytes.length,
                    point.replyAddress);
            SendHelper.publish(connection, address, point.replyAddress, bytes);
            return null;
        });
        queueMap.computeIfAbsent(address, k -> new ConcurrentQueue<>(vertx));
        queueMap.get(address).executeOrPend(trigger);
        return this;
    }


    private EventBus publishCommon(String address, Object message) {
        try {
            long seqNum = seqInc.incrementAndGet();
            byte[] bytes = MsgCodec.encode(seqNum, seqNum, message.toString(), true);
            connection.publish(address, point.replyAddress, bytes);
            log.debug("event bus publish {} msg {}", address, message.toString());
            return this;
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public EventBusProxy(Vertx vertx, Connection connection, ReceiverPoint point) {
        this.vertx = vertx;
        this.connection = connection;
        this.point = point;
    }

    @Override
    public EventBus send(String address, Object message) {
        return sendCommon(address, message, null);
    }

    @Override
    public <T> EventBus send(String address, Object message, Handler<AsyncResult<Message<T>>> replyHandler) {
        return sendCommon(address, message, replyHandler);
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


    @Override
    public <T> MessageConsumer<T> consumer(String address) {
        return null;
    }

    @Override
    public <T> MessageConsumer<T> consumer(String address, Handler<Message<T>> handler) {
        Context runContext = vertx.getOrCreateContext();
        Consumer<io.nats.client.Message> consumer = cb -> {
            String replyTo = cb.getReplyTo();
            MsgCodec decode = MsgCodec.decode(cb.getData());
            String body = decode.body;
            if (decode.isPublish) {
                Message message = new MessageProxy(body, null, null);
                runContext.runOnContext(r -> handler.handle(message));
            } else {
                String ackKey = SendHelper.buildAckKey(replyTo, address);
                Long lastAck = ackRecords.getOrDefault(ackKey, -1L);
                Long currentAck = decode.ackSeq;
                //reply ACK
                SendHelper.publish(connection, replyTo, MsgCodec.encodeAck(currentAck));
                if (currentAck > lastAck) {
                    ackRecords.put(ackKey, currentAck);
                    log.debug("event bus {} consumer msg {} ", address, decode);
                    long seq = decode.seq;
                    long replyAck = seqInc.incrementAndGet();
                    Consumer<String> replyHandler = msg -> {
                        log.debug("event bus {} reply success seq {} content {} to {}", address, seq, msg, replyTo);
                        byte[] bytes = MsgCodec.encode(replyAck, seq, msg);
                        SendHelper.publish(connection, replyTo, point.replyAddress, bytes);
                        point.putRetryReply(replyAck, () -> {
                            log.warn("event bus retry {} reply success seq {} content {} to {}", address, seq, msg, replyTo);
                            SendHelper.publish(connection, replyTo, point.replyAddress, bytes);
                        });
                    };
                    BiConsumer<Integer, String> failedHandler = (code, msg) -> {
                        log.debug("event bus {} reply failed seq {} code {} content {} to {}", address, seq, code, msg, replyTo);
                        byte[] bytes = MsgCodec.encode(replyAck, seq, code, msg);
                        SendHelper.publish(connection, replyTo, point.replyAddress, bytes);
                        point.putRetryReply(replyAck, () -> {
                            log.warn("event bus retry {} reply failed seq {} code {} content {} to {}", address, seq, code, msg, replyTo);
                            SendHelper.publish(connection, replyTo, point.replyAddress, bytes);
                        });
                    };
                    Message message = new MessageProxy(body, replyHandler, failedHandler);
                    runContext.runOnContext(r -> handler.handle(message));
                } else {
                    log.warn("event bus {} ignore message {} currentAck:{}->lastAck:{}", address, decode, currentAck, lastAck);
                }
            }
        };
        Runnable unSubscribe = point.subscribe(address, consumer);
        return new MessageConsumerProxy(unSubscribe);
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

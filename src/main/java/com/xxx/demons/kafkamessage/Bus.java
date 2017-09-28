package com.xxx.demons.kafkamessage;

import com.google.gson.Gson;

import com.xxx.demons.kafkamessage.util.KafkaFactory;
import com.xxx.demons.kafkamessage.util.KafkaUtil;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.producer.KafkaProducer;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Bus<T> {
    private final String ID = UUID.randomUUID().toString();
    private final String NAME;
    private final String REPLY_ADDRESS;
    private final Map<String, Handler<AsyncResult<Message<T>>>> replyFutures = new HashMap<>();
    //    private final Map<String, Long> timeoutTimers = new HashMap<>();
    private final Gson GSON = new Gson();
    private KafkaProducer<String, String> producer;
    private KafkaFactory kafkaFactory;
    private Vertx vertx;

    public Bus(Vertx vertx, KafkaFactory kafkaFactory, String name) {
        this.vertx = vertx;
        this.NAME = name;
        this.REPLY_ADDRESS = filterAddress("reply-" + NAME);
        this.kafkaFactory = kafkaFactory;
//        this.producer = kafkaFactory.createShareProducer(vertx, REPLY_ADDRESS);
        KafkaConsumer<String, String> consumer = kafkaFactory.createConsumer(REPLY_ADDRESS);
        consumer.subscribe(REPLY_ADDRESS).handler(record -> {
//            log.debug("kafka reply receive {}", record.value());
            Message<T> message = GSON.fromJson(record.value(), Message.class);
            message.vertx = vertx;
            log.debug("Bus {} reply seq {}", REPLY_ADDRESS, message.seq);
//            Long timer = timeoutTimers.remove(message.seq);
//            if (timer != null) vertx.cancelTimer(timer);
            replyFutures.computeIfPresent(message.seq, (seq, handler) -> {
                if (message.failed) {
                    handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, message.code, message.errorMessage)));
                } else {
                    handler.handle(Future.succeededFuture(message));
                }
                return null;
            });
        });
    }

    public KafkaConsumer<String, String> consumer(String address, Handler<Message<T>> handler) {
        address = filterAddress(address);
        log.debug("Bus {} consumer {}", REPLY_ADDRESS, address);
        KafkaConsumer<String, String> consumer = kafkaFactory.createConsumer(address);
        String finalAddress = address;
        consumer.subscribe(address).handler(record -> {
            log.debug("Bus {} consumer {} receive {}", REPLY_ADDRESS, finalAddress, record.value());
            Message<T> message = GSON.fromJson(record.value(), Message.class);
            message.vertx = vertx;
            message.wrapReceived(producer, GSON);
            handler.handle(message);
        });
        return consumer;
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
//            String finalAddress = address;
//            timeoutTimers.put(wrap.seq, vertx.setTimer(30000, l -> log.debug("time out Bus {} send {} to {} ", REPLY_ADDRESS, message, finalAddress)));
        }
        KafkaUtil.sendMessage(vertx, producer, address, GSON.toJson(wrap));
    }

    public void publish(String address, T message) {
        address = filterAddress(address);
        Message wrap = Message.wrapSend(UUID.randomUUID().toString(), REPLY_ADDRESS, message);
        KafkaUtil.sendMessage(vertx, producer, address, GSON.toJson(wrap));
    }

    public String filterAddress(String address) {
        return address.replaceAll("/", "-");
    }
}

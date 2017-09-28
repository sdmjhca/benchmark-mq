package com.xxx.demons.rabbitmessage.proxy;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;

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
    private Channel channel;
    private ReceiverPoint receiverPoint;

    public EventBusProxy(Vertx vertx, Connection connection, ReceiverPoint receiverPoint) {
        try {
            this.vertx = vertx;
            this.connection = connection;
            this.channel = connection.createChannel();
            this.channel.basicQos(0);
            this.receiverPoint = receiverPoint;
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static AMQP.BasicProperties buildSuccessProperties(String correlationId) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("success", true);
        return new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .headers(headers)
                .build();
    }


    public static AMQP.BasicProperties buildFailedProperties(String correlationId, int errorCode, String errorMessage) {
        Map<String, Object> headers = new HashMap<>();
        headers.put("success", false);
        headers.put("errorCode", errorCode);
        headers.put("errorMessage", errorMessage);
        return new AMQP.BasicProperties
                .Builder()
                .correlationId(correlationId)
                .headers(headers)
                .build();
    }

    public void reply(String replyTo, AMQP.BasicProperties replyProperties, String replyMsg, long deliveryTag) {
        try {
            channel.basicPublish("", replyTo, replyProperties, replyMsg.getBytes(Charset.defaultCharset()));
            channel.basicAck(deliveryTag, false);
        } catch (IOException e) {
            log.error("", e);
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
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .correlationId(corrId)
                    .replyTo(receiverPoint.replyQueueName)
                    .build();
            if (replyHandler != null) {
                Handler<AsyncResult<Message<T>>> receiveHandler = async -> {
                    runContext.runOnContext(r -> replyHandler.handle(async));
                };
                receiverPoint.putHandler(corrId, receiveHandler);
            }
            channel.basicPublish("", address, props, message.toString().getBytes(Charset.defaultCharset()));
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
            AMQP.BasicProperties props = new AMQP.BasicProperties
                    .Builder()
                    .build();
            channel.basicPublish("", address, props, message.toString().getBytes(Charset.defaultCharset()));
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
            channel.queueDeclare(address, false, false, true, null);
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String corrId = properties.getCorrelationId();
                    String replyTo = properties.getReplyTo();
                    long deliveryTag = envelope.getDeliveryTag();
                    java.util.function.Consumer<String> replyHandler = msg -> {
                        reply(replyTo, buildSuccessProperties(corrId), msg, deliveryTag);
                    };
                    BiConsumer<Integer, String> failedHandler = (code, msg) -> {
                        reply(replyTo, buildFailedProperties(corrId, code, msg), "", deliveryTag);
                    };
                    Message message = new MessageProxy(new String(body), replyHandler, failedHandler);
                    runContext.runOnContext(r -> handler.handle(message));
                }
            };
            String consumerTag = channel.basicConsume(address, false, consumer);
            return new MessageConsumerProxy(channel, consumerTag);
        } catch (IOException e) {
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

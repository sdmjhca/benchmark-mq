package com.xxx.demons.rabbitmessage.proxy;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;

@Component
public class ReceiverPoint {
    @Resource
    private Connection rabbitConnection;
    private Channel channel;
    public String replyQueueName;

    public final Map<String, Handler> replyHandlers = new ConcurrentHashMap<>();

    public <T> void putHandler(String key, Handler<AsyncResult<Message<T>>> handler) {
        replyHandlers.put(key, handler);
    }

    @PostConstruct
    public void initialize() throws IOException {
        this.channel = rabbitConnection.createChannel();
        this.channel.basicQos(0);
        this.replyQueueName = channel.queueDeclare().getQueue();

        channel.basicConsume(replyQueueName, true, new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                replyHandlers.computeIfPresent(properties.getCorrelationId(), (seq, handler) -> {
                    Map<String, Object> headers = properties.getHeaders();
                    boolean success = (boolean) headers.get("success");

                    if (success) {
                        Message<String> message = new MessageProxy(new String(body, Charset.defaultCharset()));
                        handler.handle(Future.succeededFuture(message));
                    } else {
                        int errorCode = (int) headers.get("errorCode");
                        String errorMessage = (String) headers.get("errorMessage");
                        handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, errorCode, errorMessage)));
                    }
                    return null;
                });
            }
        });
    }
}

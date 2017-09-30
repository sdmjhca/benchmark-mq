package com.xxx.demons.netsmessage;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ReceiverPoint {
    @Resource
    private Connection natsConnection;
    public String replyQueueName;

    public final Map<String, Handler> replyHandlers = new ConcurrentHashMap<>();

    public <T> void putHandler(String key, Handler<AsyncResult<Message<T>>> handler) {
        replyHandlers.put(key, handler);
    }

    @PostConstruct
    public void initialize() throws IOException {
        replyQueueName = UUID.randomUUID().toString();
        natsConnection.subscribe(replyQueueName, cb -> {
            log.debug("rec msg {}", cb.getData());
            String s = new String(cb.getData());
            String[] split = s.split("#");
            String successTag = split[0];
            String seqTag = split[1];
            replyHandlers.computeIfPresent(seqTag, (seq, handler) -> {
                if ("0".equals(successTag)) {
                    String body = "";
                    if (split.length == 3) body = split[2];
                    Message<String> message = new MessageProxy(body);
                    handler.handle(Future.succeededFuture(message));
                } else if ("1".equals(successTag)) {
                    int code = Integer.parseInt(split[2]);
                    String msg = split[3];
                    handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, code, msg)));
                }
                return null;
            });
        });
    }
}

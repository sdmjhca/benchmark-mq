package com.xxx.demons.netsmessage;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.eventbus.ReplyFailure;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class ReceiverPoint {
    @Resource
    private Connection natsConnection;
    String replyAddress;

    private Vertx vertx = Vertx.vertx();

    private final long PERIODIC_RETRY_TIME = 5000L;
    private final Map<Long, RetryContext> retrySendHandlers = new ConcurrentHashMap<>();

    private final Map<Long, Handler> replyHandlers = new ConcurrentHashMap<>();
    private final Map<String, Map<String, Consumer<io.nats.client.Message>>> consumerHandlers = new ConcurrentHashMap<>();

    <T> void putHandler(long seq, Handler<AsyncResult<Message<T>>> handler, Runnable retry, Runnable complete) {
        replyHandlers.put(seq, handler);
        retrySendHandlers.put(seq, new RetryContext(seq, retry, complete));
    }

    void putRetryReply(long seq, Runnable retry) {
        retrySendHandlers.put(seq, new RetryContext(seq, retry));
    }

    public static class RetryContext {
        long seq;
        long timestamp;
        Runnable retry;
        Runnable complete;
        private int times;

        void retry() {
            log.warn("{} retry times {}", seq, ++times);
            retry.run();
        }

        RetryContext(long seq, Runnable retry) {
            this.seq = seq;
            this.timestamp = System.currentTimeMillis();
            this.retry = retry;
        }


        RetryContext(long seq, Runnable retry, Runnable complete) {
            this.seq = seq;
            this.timestamp = System.currentTimeMillis();
            this.retry = retry;
            this.complete = complete;
        }
    }

    @PostConstruct
    public void initialize() throws IOException {
        replyAddress = UUID.randomUUID().toString();
        vertx.setPeriodic(PERIODIC_RETRY_TIME, l -> doRetrySend());
        natsConnection.subscribe(replyAddress, cb -> {
            MsgCodec decode = MsgCodec.decode(cb.getData());
            if (decode.isAck) {
                log.debug("receive ACK {}", decode);
                RetryContext remove = retrySendHandlers.remove(decode.ackSeq);
                if (remove != null && remove.complete != null) remove.complete.run();
            } else {
                log.debug("receive msg {}", decode);
                String replyTo = cb.getReplyTo();
                //reply ACK
                SendHelper.publish(natsConnection, replyTo, MsgCodec.encodeAck(decode.ackSeq));
                boolean successTag = decode.success;
                replyHandlers.computeIfPresent(decode.seq, (seq, handler) -> {
                    if (successTag) {
                        Message<String> message = new MessageProxy(decode.body);
                        handler.handle(Future.succeededFuture(message));
                    } else {
                        int code = decode.code;
                        String msg = decode.body;
                        handler.handle(Future.failedFuture(new ReplyException(ReplyFailure.RECIPIENT_FAILURE, code, msg)));
                    }
                    return null;
                });
            }
        });
    }

    private void doRetrySend() {
        long now = System.currentTimeMillis();
        retrySendHandlers.values().stream()
                .filter(c -> (now - c.timestamp) > PERIODIC_RETRY_TIME)
                .forEach(RetryContext::retry);
    }

    Runnable subscribe(String address, Consumer<io.nats.client.Message> handler) {
        String uniqueID = UUID.randomUUID().toString();
        consumerHandlers.computeIfAbsent(address, k -> {
            Map<String, Consumer<io.nats.client.Message>> handlerMap = new ConcurrentHashMap<>();
            natsConnection.subscribe(address, cb -> handlerMap.values().forEach(h -> h.accept(cb)));
            return handlerMap;
        });
        Map<String, Consumer<io.nats.client.Message>> handlerMap = consumerHandlers.get(address);
        handlerMap.putIfAbsent(uniqueID, handler);
        return () -> handlerMap.remove(uniqueID);
    }
}

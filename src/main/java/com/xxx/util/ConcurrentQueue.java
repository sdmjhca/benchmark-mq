package com.xxx.util;

import java.util.LinkedList;
import java.util.List;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ConcurrentQueue<T> {
    private List<Future<T>> pending = new LinkedList<>();

    private Vertx vertx;

    public ConcurrentQueue(Vertx vertx) {
        this.vertx = vertx;
    }

    public void executeOrPend(Future<T> task) {
        synchronized (ConcurrentQueue.class) {
            pending.add(task);
            if (pending.size() == 1) {
                vertx.runOnContext(r -> pending.get(0).complete());
            }
        }
    }

    public Future<T> removeAtHead() {
        return pending.remove(0);
    }

    public Future<T> take() {
        synchronized (ConcurrentQueue.class) {
            Future<T> ret = removeAtHead();
            if (!pending.isEmpty()) {
                vertx.runOnContext(r -> pending.get(0).complete());
            }
            return ret;
        }
    }
}

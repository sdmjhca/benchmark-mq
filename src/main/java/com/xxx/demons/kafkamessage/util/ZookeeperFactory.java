package com.xxx.demons.kafkamessage.util;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;

import javax.annotation.PostConstruct;

import lombok.extern.slf4j.Slf4j;

//@Component
@Slf4j
public class ZookeeperFactory {
    @Value("#{environment.ZOOKEEPER_HOST}")
    private String ZOOKEEPER_HOST;
    private static final int SESSION_TIME_OUT = 10000;
    private CuratorFramework client;

    @PostConstruct
    public void initialize() throws IOException {
        client = createWithOptions(ZOOKEEPER_HOST, new ExponentialBackoffRetry(1000, Integer.MAX_VALUE),
                SESSION_TIME_OUT, SESSION_TIME_OUT);
        client.start();
    }

    public CuratorFramework createZookeeper() {
        return client;
    }

    public static CuratorFramework createWithOptions(String connectionString, RetryPolicy retryPolicy, int connectionTimeoutMs, int sessionTimeoutMs) {
        // using the CuratorFrameworkFactory.builder() gives fine grained control
        // over creation options. See the CuratorFrameworkFactory.Builder javadoc details
        return CuratorFrameworkFactory.builder().connectString(connectionString)
                .retryPolicy(retryPolicy)
                .connectionTimeoutMs(connectionTimeoutMs)
                .sessionTimeoutMs(sessionTimeoutMs)
                // etc. etc.
                .build();
    }
}

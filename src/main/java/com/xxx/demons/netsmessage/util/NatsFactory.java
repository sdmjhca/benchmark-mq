package com.xxx.demons.netsmessage.util;


import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import io.nats.client.Connection;
import io.nats.client.ConnectionFactory;
import io.nats.client.Nats;

@Component
public class NatsFactory {
//    @Value("#{environment.HAPROXY_HOST}")
//    private String NATS_HOST;
//    @Value("#{environment.HAPROXY_PORT}")
//    private  int NATS_PORT;

    public ConnectionFactory create() {
        Properties properties = new Properties();
        properties.setProperty(Nats.PROP_VERBOSE,"true");
        properties.setProperty(Nats.PROP_URL,"nats://192.168.8.80:4222");
        properties.setProperty(Nats.PROP_SERVERS,"nats://192.168.8.80:4222");
        properties.setProperty(Nats.PROP_MAX_RECONNECT,"10000");
        properties.setProperty(Nats.PROP_RECONNECT_WAIT,"2000");
//        ConnectionFactory cf = new ConnectionFactory("nats://172.31.28.84:4224");
        ConnectionFactory cf = new ConnectionFactory(properties);
        return cf;
    }

    public Connection createConnection() throws IOException, TimeoutException {
        return create().createConnection();
    }
}

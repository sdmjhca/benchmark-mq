package com.xxx.demons.rabbitmessage.util;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@Component
public class RabbitMQFactory {
    public ConnectionFactory create() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("192.168.8.77");
        factory.setPort(5672);
        factory.setUsername("admin");
        factory.setPassword("admin");
        return factory;
    }

    public Connection createConnection() throws IOException, TimeoutException {
        return create().newConnection();
    }
}

package com.xxx.demons.netsmessage;

import java.io.IOException;

import io.nats.client.Connection;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SendHelper {
    public static void publish(Connection connection, String subject, String reply, byte[] data) {
        try {
            connection.publish(subject, reply, data);
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }


    public static void publish(Connection connection, String subject, byte[] data) {
        try {
            connection.publish(subject, data);
        } catch (IOException e) {
            log.error("", e);
            throw new RuntimeException(e.getMessage());
        }
    }

    public static String buildAckKey(String replyTo, String address) {
        return replyTo + "-" + address;
    }
}

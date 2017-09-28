package com.xxx.demons.roketmessage.util;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RocketUtil {
    public static void sendMessage(DefaultMQProducer producer, String topic, String msg) {
        log.debug("[RocketUtil][Try Send] send message {} to topic {}", msg, topic);
        Message message = new Message(topic, "1", msg.getBytes(Charset.defaultCharset()));
        try {
            producer.send(message);
        } catch (MQClientException e) {
            log.error("", e);
            throw new RuntimeException("MQClientException: " + e.getMessage());
        } catch (RemotingException e) {
            log.error("", e);
            throw new RuntimeException("RemotingException: " + e.getMessage());
        } catch (MQBrokerException e) {
            log.error("", e);
            throw new RuntimeException("MQBrokerException: " + e.getMessage());
        } catch (InterruptedException e) {
            log.error("", e);
            throw new RuntimeException("InterruptedException: " + e.getMessage());
        }
        log.debug("[RocketUtil][Send] success send message {} to topic {}", msg, topic);
    }

    public static void sendReplyMessage(DefaultMQProducer producer, String topic, String msg) {
        log.debug("[RocketUtil][Reply] send message {} to topic {}", msg, topic);
        Message message = new Message(topic, "1", msg.getBytes(Charset.defaultCharset()));
        try {
            producer.send(message);
        } catch (MQClientException e) {
            log.error("", e);
            throw new RuntimeException("MQClientException: " + e.getMessage());
        } catch (RemotingException e) {
            log.error("", e);
            throw new RuntimeException("RemotingException: " + e.getMessage());
        } catch (MQBrokerException e) {
            log.error("", e);
            throw new RuntimeException("MQBrokerException: " + e.getMessage());
        } catch (InterruptedException e) {
            log.error("", e);
            throw new RuntimeException("InterruptedException: " + e.getMessage());
        }
        log.debug("[RocketUtil][Reply] success send message {} to topic {}", msg, topic);
    }
}

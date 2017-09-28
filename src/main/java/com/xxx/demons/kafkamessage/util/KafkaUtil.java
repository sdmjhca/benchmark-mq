package com.xxx.demons.kafkamessage.util;

import com.xxx.client.Starter;
import com.xxx.util.Asserts;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
@Scope(BeanDefinition.SCOPE_SINGLETON)
public class KafkaUtil {
    public static void sendMessage(Vertx vertx, KafkaProducer<String, String> producer, String topic, String res) {
        Asserts.assertNotNull(topic, RuntimeException::new);
        Asserts.assertNotNull(res, RuntimeException::new);
        Asserts.assertNotNull(producer, RuntimeException::new);
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, res);
        log.debug("[KafkaUtil][Try Send] kafka send message {} to topic {}", res, topic);
        Starter.senderMeter.mark();
        producer.write(record, done -> {
            if (done.succeeded()) {
                log.debug("[KafkaUtil][Send] success kafka send message {} to topic {}", res, topic);
            } else {
                log.error("[KafkaUtil][Send] kafka send message {} to topic {} error: {}", res, topic, done.cause());
            }
        });
    }

    public static void sendReplyMessage(Vertx vertx, KafkaProducer<String, String> producer, String topic, String res) {
        Asserts.assertNotNull(topic, RuntimeException::new);
        Asserts.assertNotNull(res, RuntimeException::new);
        Asserts.assertNotNull(producer, RuntimeException::new);
        KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topic, res);
        log.debug("[KafkaUtil][Reply] kafka reply message {} to topic {}", res, topic);
        producer.write(record, done -> {
            if (done.succeeded()) {
                log.debug("[KafkaUtil][Reply] success kafka reply message {} to topic {}", res, topic);
            } else {
                log.error("[KafkaUtil][Reply] kafka reply message {} to topic {} error: {}", done.cause());
            }
        });
    }
}